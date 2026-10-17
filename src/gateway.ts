import {
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";
import { createHash, randomUUID } from "node:crypto";
import Redis from "ioredis";
import { SessionStore, type Turn } from "./session-store.ts";

// MVP 阶段还没有接入真实的 AI 推理，这里用固定的 token 列表模拟"逐字吐字"的效果。
const MOCK_TOKENS = ["Hello", ", ", "world", "!"];

const GROUP_NAME = "chat-workers";

export interface GatewayEvent {
  type:
    | "dispatched"
    | "claimed"
    | "completed"
    | "redelivered"
    | "rejected"
    | "failed";
  // "rejected" 没有 jobId：请求在真正进入队列之前就被拒绝了，根本没有被派发过。
  jobId?: string;
  consumerName?: string;
  // 在 "dispatched" 事件上表示"路由到了哪个 worker 的分片"；在其它事件上表示"是哪个 worker 在处理"。
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
  // 每个会话最多保留多少轮历史（滑动窗口，见 docs/adr/0001）。默认 20，最小 1。
  // 超出的最早几轮会从 Redis 里真正删掉，送进推理的上下文因此也有上限。
  maxHistoryTurns?: number;
}

export interface GatewayServer extends Server {
  // 模拟"第 index 个 worker 突然崩溃"：让它立刻停止处理，且不会 ack 手上正在处理的任务，
  // 从而让 Consumer Group 的 pending 列表里留下一条没确认的消息，等待被别的 worker 重新 claim。
  killWorker(index: number): void;
}

// mock 推理：回复里会带上这个会话之前说过的话，用来证明历史真的被带进了推理。
// 刻意不回显当前这一轮的 prompt，这样"回复里出现某句话"只可能来自历史。
// 开头的 "Seen N earlier turn(s)" 是推理实际拿到的上下文轮数——裁剪之后它有上限。
function mockInfer(history: Turn[]): string[] {
  const reply = [`Seen ${history.length} earlier turn(s). `];
  if (history.length === 0) {
    reply.push("No earlier context.");
  } else {
    reply.push("Earlier you said: ", history.map((t) => t.user).join(" | "));
  }
  return reply;
}

async function readChatRequest(
  req: IncomingMessage,
): Promise<{ sessionId?: string; prompt: string }> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  try {
    const body = JSON.parse(Buffer.concat(chunks).toString("utf-8"));
    return {
      sessionId:
        typeof body.sessionId === "string" && body.sessionId
          ? body.sessionId
          : undefined,
      prompt: typeof body.prompt === "string" ? body.prompt : "",
    };
  } catch {
    return { prompt: "" };
  }
}

// 会话亲和性用的 rendezvous（最高随机权重）哈希：对每个候选 worker 算 hash(sessionId, worker)，
// 取分数最高的那个。同一个 sessionId 永远选中同一个 worker；
// 某个 worker 下线时，只有原本落在它身上的会话会换 worker，其它会话不受影响。
function rendezvousScore(sessionId: string, worker: number): number {
  return createHash("sha1").update(`${sessionId}#${worker}`).digest().readUInt32BE(0);
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

  const maxHistoryTurns = Math.max(1, options.maxHistoryTurns ?? 20);

  const dispatchPool = new RedisPool(redisUrl, dispatchPoolSize);
  const sessionStore = new SessionStore(redisUrl, maxHistoryTurns);

  // 每个 worker 有自己的一条 Stream（分片），而不是所有 worker 抢同一条：
  // 只有这样网关才能决定"这个任务交给谁"，会话亲和性才有可能实现。
  // 每个分片仍然用 Consumer Group，ack / pending / 崩溃后重新认领的机制和之前完全一样。
  const shardKeys = Array.from({ length: workerCount }, (_, i) => `${streamKey}:${i}`);
  // 每个分片上派发了但还没完成的任务数，用来给没有 session 的请求挑最空闲的 worker。
  const shardLoad: number[] = Array.from({ length: workerCount }, () => 0);

  // 请求进来后先去排队，worker 真正处理完才知道要把结果写回哪个 HTTP 响应，
  // 所以要按 requestId 记住"是谁在等这个结果"。这个映射被所有 worker 共享：
  // 不管最终是哪个 worker 完成了任务，都能找到正确的客户端连接。
  // dispatched 是"网关已确认派发"的信号：worker 可能比 XADD 的回复更早读到任务，
  // 所以 worker 要等它 resolve 之后才算 claimed，保证可观测事件顺序始终是 dispatched -> claimed。
  const pendingResponses = new Map<
    string,
    { res: ServerResponse; dispatched: Promise<void>; markDispatched: () => void }
  >();

  // 有多少个任务已经派发但还没完成（不管是正在被某个 worker 处理，还是还在排队）。
  // 用来算 queueDepth：超过 workerCount 的那部分就是"排在队列里、还没轮到的"。
  let unfinishedCount = 0;

  let stopped = false;
  // 每个 worker 一个独立的"已被杀死"标记，killWorker(i) 只影响第 i 个。
  // 路由时它也充当"健康检查"：生产环境里这份存活名单来自心跳/注册中心，这里用进程内标记代替。
  const killed: boolean[] = Array.from({ length: workerCount }, () => false);

  function liveWorkers(): number[] {
    const live = killed.flatMap((dead, i) => (dead ? [] : [i]));
    // 全部挂了也得有个去处：任务会留在分片里，等 worker 恢复（或被判定 stale 后认领）。
    return live.length > 0 ? live : killed.map((_, i) => i);
  }

  // 路由：有 session 的请求按 rendezvous 哈希固定到某个存活的 worker（亲和性）；
  // 没有 session 的请求没有"必须去哪"的理由，就交给当前负载最低的 worker。
  function pickShard(sessionId: string | undefined): number {
    const live = liveWorkers();
    if (sessionId) {
      let best = live[0];
      let bestScore = -1;
      for (const i of live) {
        const score = rendezvousScore(sessionId, i);
        if (score > bestScore) [best, bestScore] = [i, score];
      }
      return best;
    }
    return live.reduce((a, b) => (shardLoad[b] < shardLoad[a] ? b : a));
  }

  async function ensureGroups(connection: Redis): Promise<void> {
    for (const key of shardKeys) {
      try {
        // 从 "0" 而不是 "$" 开始：请求在 worker 建好组之前就可能已经 XADD 了，
        // 用 "$" 会让这些先到的任务永远不被投递给组，客户端就一直挂着。
        await connection.xgroup("CREATE", key, GROUP_NAME, "0", "MKSTREAM");
      } catch (err) {
        // BUSYGROUP 表示组已经存在，忽略；其它错误才需要真正关心。
        if (!(err as Error).message.includes("BUSYGROUP")) throw err;
      }
    }
  }

  // 尝试从各个分片的 pending 列表里"认领"一条空闲超过 staleAfterMs 的消息——
  // 也就是别的 worker 已经 claim 但还没 ack、且已经沉默太久的任务，视为那个 worker 已经挂了。
  // 要扫所有分片，而不只是自己的：挂掉的正是别的分片的主人。
  async function claimStale(
    connection: Redis,
    consumerName: string,
  ): Promise<[string, [string, string[]]] | null> {
    for (const key of shardKeys) {
      const pendingList = (await connection.xpending(
        key,
        GROUP_NAME,
        "IDLE",
        staleAfterMs,
        "-",
        "+",
        1,
      )) as [string, string, number, number][] | null;
      if (!pendingList || pendingList.length === 0) continue;

      const [staleId] = pendingList[0];
      const claimed = (await connection.xclaim(
        key,
        GROUP_NAME,
        consumerName,
        staleAfterMs,
        staleId,
      )) as [string, string[]][] | null;
      if (!claimed || claimed.length === 0) continue;

      onEvent({ type: "redelivered", jobId: staleId, consumerName });
      return [key, claimed[0]];
    }
    return null;
  }

  async function readOne(
    connection: Redis,
    consumerName: string,
    key: string,
    blockMs: number | null,
  ): Promise<[string, string[]] | null> {
    const blockArgs = blockMs === null ? [] : ["BLOCK", blockMs];
    const result = (await connection.xreadgroup(
      "GROUP",
      GROUP_NAME,
      consumerName,
      ...(blockArgs as ["BLOCK", number]),
      "COUNT",
      1,
      "STREAMS",
      key,
      ">",
    )) as [string, [string, string[]][]][] | null;
    return result ? result[0][1][0] : null;
  }

  function finishJob(requestId: string, shard: number): void {
    pendingResponses.delete(requestId);
    unfinishedCount -= 1;
    shardLoad[shard] -= 1;
  }

  async function processJob(
    jobStream: string,
    jobId: string,
    fields: string[],
    connection: Redis,
    consumerName: string,
    index: number,
  ): Promise<void> {
    const fieldMap: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) {
      fieldMap[fields[i]] = fields[i + 1];
    }
    // 按 requestId 而不是 jobId 找客户端连接：worker 可能在 XADD 的回复回到网关之前
    // 就已经从 Stream 里读到了这个任务，那时 jobId 还没人知道、没法提前登记。
    const requestId = fieldMap.requestId;
    const shard = Number(fieldMap.shard);
    const pending = pendingResponses.get(requestId);
    if (!pending) return; // 不是本实例派发的任务，理论上不会发生
    await pending.dispatched;
    const { res } = pending;

    onEvent({ type: "claimed", jobId, consumerName });

    // claim 到任务之后、真正开始写响应之前留一个小窗口（默认 0，生产环境不需要）。
    // 崩溃必须发生在这个窗口里、而不是写了一半的时候——否则别的 worker 重新执行整个任务时，
    // 客户端会收到"前一个 worker 写的半截 + 新 worker 完整写的一遍"，内容就重复了。
    // 只要确保"开始真正处理"这件事本身是原子的（要么完全没开始，要么保证能走到底），
    // 重新投递就是安全的全量重试，不需要处理"断点续传"这种更复杂的情形。
    if (claimToProcessGraceMs > 0) await delay(claimToProcessGraceMs);
    if (killed[index]) return;

    // 读历史、组装上下文属于"推理"的一部分，放在 worker 里做，而不是在网关派发时做：
    // 同一个 session 的请求被亲和性路由到同一个 worker、依次执行，
    // 后一个请求开始推理时，前一个请求的这一轮已经写进历史了。
    // 如果在网关里提前读，几个同时到达的请求会读到同一份"旧"历史。
    const jobTokens: string[] = fieldMap.tokens
      ? JSON.parse(fieldMap.tokens)
      : mockInfer(await sessionStore.getHistory(fieldMap.sessionId));
    const jobDelayMs = Number(fieldMap.tokenDelayMs);

    let lastBeat = Date.now();
    for (const token of jobTokens) {
      res.write(`data: ${token}\n\n`);
      if (jobDelayMs > 0) await delay(jobDelayMs);
      // 心跳：staleAfterMs 衡量的是"距离上次活动多久"，不是"距离 claim 多久"。
      // 活着的 worker 推流时间可以任意长，所以要定期把自己这条 pending 消息的 idle 清零，
      // 否则别的 worker 会误判它挂了、把任务抢走并往同一个响应里重复写。
      // 真正挂掉的 worker 不会再心跳，仍然会被及时判定为 stale。
      if (!killed[index] && Date.now() - lastBeat >= staleAfterMs / 3) {
        await connection.xclaim(
          jobStream,
          GROUP_NAME,
          consumerName,
          0,
          jobId,
          "JUSTID",
        );
        lastBeat = Date.now();
      }
      if (killed[index]) {
        // 已经往客户端推了一部分，不能再让别的 worker 重推整个任务（内容会重复），
        // 所以走"明确失败"：告诉客户端可重试、结束流，并 ack 掉避免被反复重新认领。
        res.write(
          `event: error\ndata: ${JSON.stringify({ error: "worker_failed", retryable: true })}\n\n`,
        );
        res.end();
        finishJob(requestId, shard);
        await connection.xack(jobStream, GROUP_NAME, jobId);
        onEvent({ type: "failed", jobId, consumerName });
        return;
      }
    }
    // 先把这一轮写进会话历史，再告诉客户端结束：客户端一收到 [DONE] 就可能立刻发下一轮，
    // 如果顺序反过来，下一轮可能读不到刚完成的这一轮。
    if (fieldMap.sessionId) {
      await sessionStore.appendTurn(fieldMap.sessionId, {
        user: fieldMap.prompt ?? "",
        assistant: jobTokens.join(""),
      });
    }
    res.write("data: [DONE]\n\n");
    res.end();

    finishJob(requestId, shard);
    await connection.xack(jobStream, GROUP_NAME, jobId);
    onEvent({ type: "completed", jobId, consumerName });
  }

  async function runWorker(index: number): Promise<void> {
    const consumerName = `worker-${index}`;
    const ownShard = shardKeys[index];
    // XREADGROUP 的 BLOCK 模式会占住连接，每个 worker 必须用自己独立的连接。
    const connection = new Redis(redisUrl);
    await ensureGroups(connection);

    while (!stopped) {
      if (killed[index]) break;

      try {
        // 1. 先检查有没有被中断、需要重新认领的旧任务（任意分片上）。
        let found = await claimStale(connection, consumerName);

        // 2. 没有的话，从自己的分片读一条全新的消息。
        if (!found) {
          const entry = await readOne(connection, consumerName, ownShard, 200);
          if (entry) found = [ownShard, entry];
        }

        // 3. 自己的分片也空闲：帮已经下线的 worker 清空它分片里还没人读过的消息。
        //    路由不会再往那里派新任务，但下线之前已经派过去、还没被读走的任务
        //    不在任何人的 pending 列表里，第 1 步的 stale 认领找不到它们。
        if (!found) {
          for (let j = 0; j < workerCount && !found; j += 1) {
            if (j === index || !killed[j]) continue;
            const entry = await readOne(connection, consumerName, shardKeys[j], null);
            if (entry) found = [shardKeys[j], entry];
          }
        }

        if (!found || killed[index]) continue;

        const [jobStream, [jobId, fields]] = found;
        await processJob(jobStream, jobId, fields, connection, consumerName, index);
      } catch (err) {
        if (stopped) break;
        // 分片的 Stream 被删掉（比如测试或运维清理了 key）会报 NOGROUP：重建消费组后继续。
        if ((err as Error).message.includes("NOGROUP")) {
          await ensureGroups(connection).catch(() => {});
          continue;
        }
        console.error(`[worker-${index}] error:`, err);
        await delay(100);
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
      // 一旦达到 workerCount + maxQueueDepth 的总容量，直接拒绝——不让 Redis Stream
      // 无限变长，这是"持续超载也不会无限增长"的关键，而不是先收下来再想办法。
      // 注意不能先对 (unfinishedCount - workerCount) 做 Math.max(0, ...) 再比较：
      // 空闲时 unfinishedCount < workerCount，clamp 后会变成 0，
      // 当 maxQueueDepth 也配置成 0（"不排队，但允许跑满 workerCount 个"）时，
      // 0 >= 0 恒成立，会导致空闲网关也把所有请求 429 掉。
      if (unfinishedCount >= workerCount + maxQueueDepth) {
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

      const requestId = randomUUID();
      void (async () => {
        let shard: number | undefined;
        try {
          const { sessionId, prompt } = await readChatRequest(req);
          // 选分片和占分片负载必须在同一个同步片段里完成，中间不能有 await，
          // 否则并发请求会看到同一个"最空闲"的分片、全挤过去。
          shard = pickShard(sessionId);
          shardLoad[shard] += 1;

          // 有 sessionId（且外部没有显式指定 tokens）时，不在这里定好 tokens，
          // 而是让 worker 读历史后现场推理；否则直接把要吐的 tokens 放进任务里。
          const inferInWorker = sessionId !== undefined && !options.tokens;

          // 请求本身不再直接跑 mock 推理，而是把"要跑什么"交给 Redis Stream，
          // 由 worker 们取出来执行——对客户端来说接口和行为完全不变。
          let markDispatched!: () => void;
          const dispatched = new Promise<void>((r) => (markDispatched = r));
          pendingResponses.set(requestId, { res, dispatched, markDispatched });
          const jobId = await dispatchPool.next().xadd(
            shardKeys[shard],
            "*",
            ...(inferInWorker ? [] : ["tokens", JSON.stringify(tokens)]),
            "tokenDelayMs",
            String(tokenDelayMs),
            "requestId",
            requestId,
            "shard",
            String(shard),
            ...(sessionId ? ["sessionId", sessionId, "prompt", prompt] : []),
          );
          if (!jobId) return;
          onEvent({
            type: "dispatched",
            jobId,
            queueDepth,
            consumerName: `worker-${shard}`,
          });
          markDispatched();
        } catch {
          pendingResponses.delete(requestId);
          // 派发前出错（比如 Redis 不可用）：占着的名额要还回去，并明确告诉客户端失败了。
          unfinishedCount -= 1;
          if (shard !== undefined) shardLoad[shard] -= 1;
          res.write(
            `event: error\ndata: ${JSON.stringify({ error: "dispatch_failed", retryable: true })}\n\n`,
          );
          res.end();
        }
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
      sessionStore.disconnect();
    });
  });

  return server;
}
