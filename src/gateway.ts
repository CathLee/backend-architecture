import { createServer, type Server, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import Redis from "ioredis";

// MVP 阶段还没有接入真实的 AI 推理，这里用固定的 token 列表模拟"逐字吐字"的效果。
const MOCK_TOKENS = ["Hello", ", ", "world", "!"];

const GROUP_NAME = "chat-workers";

export interface GatewayEvent {
  type: "dispatched" | "claimed" | "completed" | "redelivered" | "rejected";
  // "rejected" 没有 jobId：请求在真正进入队列之前就被拒绝了，根本没有被派发过。
  jobId?: string;
  consumerName?: string;
  // 只在 "dispatched" 事件上有意义：这个请求派发时，前面已经有多少个还没完成的任务
  // 超出了 worker 池的容量（即它大概要排多久的队）。0 表示能立刻被某个 worker 处理。
  queueDepth?: number;
}

export interface StartGatewayOptions {
  // 每个 token 之间等待的毫秒数，默认 0（不等待），方便大多数测试跑得快。
  tokenDelayMs?: number;
  // 允许外部自定义要流式输出的 token 序列。
  tokens?: string[];
  // 可观测性钩子：外部可以观察"派发""worker claim 到任务""worker 完成""任务被重新投递"
  // 这几个时刻，用来证明队列/重试机制真的在工作，而不用在测试里直接戳 Redis 内部状态。
  onEvent?: (event: GatewayEvent) => void;
  // 每个网关实例默认用随机生成的独立 Stream key，避免并行跑的多个实例
  // （比如并行执行的测试文件）互相偷对方的任务；生产环境可以传固定 key 复用。
  streamKey?: string;
  redisUrl?: string;
  // Consumer Group 里有几个独立的 worker 同时消费。默认 1（等价于上一张票的单 worker）。
  workerCount?: number;
  // 一条消息被某个 worker claim 之后，如果超过这么久还没被 ack，
  // 就认为那个 worker"挂了"，允许被别的 worker 重新 claim 走。
  consumerStaleAfterMs?: number;
  // claim 到任务之后、真正开始处理之前人为等待的毫秒数。默认 0（生产环境不需要），
  // 测试里调大一点可以稳定地在"崩溃模拟"落地之前留出可观察、可操作的窗口。
  claimToProcessGraceMs?: number;
  // 允许排队等待的任务数上限（不算正在被 worker 处理的那些）。默认不限制。
  // 一旦达到 workerCount + maxQueueDepth 的总容量，新请求会被直接拒绝（429），
  // 而不是无限堆积在 Redis Stream 里——这是防止内存/延迟无限增长的关键。
  maxQueueDepth?: number;
  // 负责派发请求（XADD）的连接池大小。默认 1。调大可以避免所有并发请求的
  // 派发命令都挤在同一条连接上排队——这是和 workerCount（消费者连接数）分开的
  // 另一组连接，因为派发是非阻塞命令，天然适合池化、轮询分摊。
  dispatchPoolSize?: number;
}

export interface GatewayServer extends Server {
  // 模拟"第 index 个 worker 突然崩溃"：让它立刻停止处理，且不会 ack 手上正在处理的任务，
  // 从而让 Consumer Group 的 pending 列表里留下一条没确认的消息，等待被别的 worker 重新 claim。
  killWorker(index: number): void;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// 一组各自独立的 Redis 连接，轮询分配给调用方——用于非阻塞命令（这里是 XADD），
// 避免所有并发请求的派发都挤在同一条连接上排队等待。
class RedisPool {
  private readonly connections: Redis[];
  private cursor = 0;

  constructor(redisUrl: string, size: number) {
    this.connections = Array.from({ length: size }, () => new Redis(redisUrl));
  }

  next(): Redis {
    const connection = this.connections[this.cursor % this.connections.length];
    this.cursor += 1;
    return connection;
  }

  disconnectAll(): void {
    for (const connection of this.connections) connection.disconnect();
  }
}

export function startGateway(options: StartGatewayOptions = {}): GatewayServer {
  const tokenDelayMs = options.tokenDelayMs ?? 0;
  const tokens = options.tokens ?? MOCK_TOKENS;
  const onEvent = options.onEvent ?? (() => {});
  const streamKey = options.streamKey ?? `chat:jobs:${randomUUID()}`;
  const workerCount = options.workerCount ?? 1;
  const staleAfterMs = options.consumerStaleAfterMs ?? 5000;
  const claimToProcessGraceMs = options.claimToProcessGraceMs ?? 0;
  const maxQueueDepth = options.maxQueueDepth ?? Infinity;
  const dispatchPoolSize = options.dispatchPoolSize ?? 1;
  // 显式用 127.0.0.1 而不是 localhost：本机上 localhost 会先解析到 IPv6 的 ::1，
  // 而 Docker 的端口映射走的是 IPv4，解析到 ::1 会导致连接悬挂超时。
  const redisUrl = options.redisUrl ?? "redis://127.0.0.1:6379";

  const dispatchPool = new RedisPool(redisUrl, dispatchPoolSize);

  // 请求进来后先去排队，worker 真正处理完才知道要把结果写回哪个 HTTP 响应，
  // 所以要按 jobId 记住"是谁在等这个结果"。这个映射被所有 worker 共享：
  // 不管最终是哪个 worker 完成了任务，都能找到正确的客户端连接。
  const pendingResponses = new Map<string, ServerResponse>();

  // 有多少个任务已经派发但还没完成（不管是正在被某个 worker 处理，还是还在排队）。
  // 用来算 queueDepth：超过 workerCount 的那部分就是"排在队列里、还没轮到的"。
  let unfinishedCount = 0;

  let stopped = false;
  // 每个 worker 一个独立的"已被杀死"标记，killWorker(i) 只影响第 i 个。
  const killed: boolean[] = Array.from({ length: workerCount }, () => false);

  async function ensureGroup(connection: Redis): Promise<void> {
    try {
      await connection.xgroup("CREATE", streamKey, GROUP_NAME, "$", "MKSTREAM");
    } catch (err) {
      // BUSYGROUP 表示组已经存在，忽略；其它错误才需要真正关心。
      if (!(err as Error).message.includes("BUSYGROUP")) throw err;
    }
  }

  // 尝试从 pending 列表里"认领"一条空闲超过 staleAfterMs 的消息——
  // 也就是别的 worker 已经 claim 但还没 ack、且已经沉默太久的任务，视为那个 worker 已经挂了。
  async function claimStale(
    connection: Redis,
    consumerName: string,
  ): Promise<[string, string[]] | null> {
    const pendingList = (await connection.xpending(
      streamKey,
      GROUP_NAME,
      "IDLE",
      staleAfterMs,
      "-",
      "+",
      1,
    )) as [string, string, number, number][] | null;
    if (!pendingList || pendingList.length === 0) return null;

    const [staleId] = pendingList[0];
    const claimed = (await connection.xclaim(
      streamKey,
      GROUP_NAME,
      consumerName,
      staleAfterMs,
      staleId,
    )) as [string, string[]][] | null;
    if (!claimed || claimed.length === 0) return null;

    onEvent({ type: "redelivered", jobId: staleId, consumerName });
    return claimed[0];
  }

  async function processJob(
    jobId: string,
    fields: string[],
    connection: Redis,
    consumerName: string,
    index: number,
  ): Promise<void> {
    onEvent({ type: "claimed", jobId, consumerName });

    // claim 到任务之后、真正开始写响应之前留一个小窗口（默认 0，生产环境不需要）。
    // 崩溃必须发生在这个窗口里、而不是写了一半的时候——否则别的 worker 重新执行整个任务时，
    // 客户端会收到"前一个 worker 写的半截 + 新 worker 完整写的一遍"，内容就重复了。
    // 只要确保"开始真正处理"这件事本身是原子的（要么完全没开始，要么保证能走到底），
    // 重新投递就是安全的全量重试，不需要处理"断点续传"这种更复杂的情形。
    if (claimToProcessGraceMs > 0) await delay(claimToProcessGraceMs);
    if (killed[index]) return;

    const res = pendingResponses.get(jobId);
    if (!res) return; // 不是本实例派发的任务，理论上不会发生

    const fieldMap: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) {
      fieldMap[fields[i]] = fields[i + 1];
    }
    const jobTokens: string[] = JSON.parse(fieldMap.tokens);
    const jobDelayMs = Number(fieldMap.tokenDelayMs);

    for (const token of jobTokens) {
      res.write(`data: ${token}\n\n`);
      if (jobDelayMs > 0) await delay(jobDelayMs);
    }
    res.write("data: [DONE]\n\n");
    res.end();

    pendingResponses.delete(jobId);
    await connection.xack(streamKey, GROUP_NAME, jobId);
    unfinishedCount -= 1;
    onEvent({ type: "completed", jobId, consumerName });
  }

  async function runWorker(index: number): Promise<void> {
    const consumerName = `worker-${index}`;
    // XREADGROUP 的 BLOCK 模式会占住连接，每个 worker 必须用自己独立的连接。
    const connection = new Redis(redisUrl);
    await ensureGroup(connection);

    while (!stopped) {
      if (killed[index]) return;

      //  先检查有没有被中断、需要重新认领的旧任务
      const staleEntry = await claimStale(connection, consumerName);
      let entry = staleEntry;

      // 如果没有需要重新认领的旧任务（if (!entry)），才去走正常路径——用 XREADGROUP 从 Stream 里读一条全新的、还没被任何人处理过的消息
      if (!entry) {
        const result = (await connection.xreadgroup(
          "GROUP",
          GROUP_NAME,
          consumerName,
          "BLOCK",
          200,
          "COUNT",
          1,
          "STREAMS",
          streamKey,
          ">",
        )) as [string, [string, string[]][]][] | null;
        if (!result) continue;
        entry = result[0][1][0];
      }

      if (killed[index]) return;

      const [jobId, fields] = entry;
      try {
        await processJob(jobId, fields, connection, consumerName, index);
      } catch (err) {
        console.error(`[DEBUG worker-${index}] processJob threw:`, err);
      }
    }

    connection.disconnect();
  }

  const workerPromises = Array.from({ length: workerCount }, (_, i) =>
    runWorker(i),
  );

  const server = createServer((req, res) => {
    if (req.method === "POST" && req.url === "/chat") {
      // 准入控制：在真正写 SSE 响应头、真正派发到 Redis 之前就决定接不接这个请求。
      // 一旦当前排队深度已经达到上限，直接拒绝——不让 Redis Stream 无限变长，
      // 这是"持续超载也不会无限增长"的关键，而不是先收下来再想办法。
      const currentQueueDepth = Math.max(0, unfinishedCount - workerCount);
      if (currentQueueDepth >= maxQueueDepth) {
        res.writeHead(429, {
          "content-type": "application/json",
          "retry-after": "1",
        });
        res.end(JSON.stringify({ error: "overloaded", retryAfterSeconds: 1 }));
        onEvent({ type: "rejected" });
        return;
      }

      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
      });

      // 准入名额要在这里、同步地占住，不能等 XADD 这个异步操作完成之后再加计数——
      // 否则并发到达的好几个请求会在各自的 XADD 都还没 resolve 之前，
      // 读到同一个"还没涨上去"的 unfinishedCount，导致准入检查形同虚设。
      unfinishedCount += 1;
      const queueDepth = Math.max(0, unfinishedCount - workerCount);

      void (async () => {
        // 请求本身不再直接跑 mock 推理，而是把"要跑什么"交给 Redis Stream，
        // 由 worker 们取出来执行——对客户端来说接口和行为完全不变。
        const jobId = await dispatchPool.next().xadd(
          streamKey,
          "*",
          "tokens",
          JSON.stringify(tokens),
          "tokenDelayMs",
          String(tokenDelayMs),
        );
        if (!jobId) return;
        pendingResponses.set(jobId, res);
        onEvent({ type: "dispatched", jobId, queueDepth });
      })();
      return;
    }

    res.writeHead(404);
    res.end();
  }) as GatewayServer;

  server.killWorker = (index: number) => {
    killed[index] = true;
  };

  server.on("close", () => {
    stopped = true;
    void Promise.all(workerPromises).then(() => {
      dispatchPool.disconnectAll();
    });
  });

  return server;
}
