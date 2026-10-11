import { createServer, type Server, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import Redis from "ioredis";

// MVP 阶段还没有接入真实的 AI 推理，这里用固定的 token 列表模拟"逐字吐字"的效果。
const MOCK_TOKENS = ["Hello", ", ", "world", "!"];

export interface GatewayEvent {
  type: "dispatched" | "completed";
  jobId: string;
}

export interface StartGatewayOptions {
  // 每个 token 之间等待的毫秒数，默认 0（不等待），方便大多数测试跑得快。
  tokenDelayMs?: number;
  // 允许外部自定义要流式输出的 token 序列。
  tokens?: string[];
  // 可观测性钩子：外部可以观察"请求被派发到队列""worker 处理完成"这两个时刻，
  // 用来证明请求确实走了 Redis Stream，而不用在测试里直接戳 Redis 内部状态。
  onEvent?: (event: GatewayEvent) => void;
  // 每个网关实例默认用随机生成的独立 Stream key，避免并行跑的多个实例
  // （比如并行执行的测试文件）互相偷对方的任务；生产环境可以传固定 key 复用。
  streamKey?: string;
  redisUrl?: string;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function startGateway(options: StartGatewayOptions = {}): Server {
  const tokenDelayMs = options.tokenDelayMs ?? 0;
  const tokens = options.tokens ?? MOCK_TOKENS;
  const onEvent = options.onEvent ?? (() => {});
  const streamKey = options.streamKey ?? `chat:jobs:${randomUUID()}`;
  // 显式用 127.0.0.1 而不是 localhost：本机上 localhost 会先解析到 IPv6 的 ::1，
  // 而 Docker 的端口映射走的是 IPv4，解析到 ::1 会导致连接悬挂超时。
  const redisUrl = options.redisUrl ?? "redis://127.0.0.1:6379";

  // XREAD 的 BLOCK 模式会占住连接直到有数据或超时，不能和普通命令共用一条连接，
  // 所以派发请求（XADD）和消费任务（XREAD）各用自己的连接。
  const redis = new Redis(redisUrl);
  const consumerConnection = new Redis(redisUrl);

  // 请求进来后先去排队，worker 真正处理完才知道要把结果写回哪个 HTTP 响应，
  // 所以要按 jobId 记住"是谁在等这个结果"。
  const pendingResponses = new Map<string, ServerResponse>();

  let stopped = false;

  async function runWorker(): Promise<void> {
    // "$" 只在第一次调用时代表"从现在开始"；之后改用上一条消息的 ID 继续往后读，
    // 避免每次都重新解析"$"而在两次调用之间的空隙里漏掉刚好插入的新消息。
    let lastId = "$";

    while (!stopped) {
      const result = await consumerConnection.xread(
        "BLOCK",
        200,
        "STREAMS",
        streamKey,
        lastId,
      );
      if (!result) continue;

      const streams = result as unknown as [string, [string, string[]][]][];
      for (const [, entries] of streams) {
        for (const [jobId, fields] of entries) {
          lastId = jobId;

          const res = pendingResponses.get(jobId);
          if (!res) continue; // 不是本实例派发的任务（理论上不会发生，因为 stream key 是独立的）

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
          onEvent({ type: "completed", jobId });
        }
      }
    }
  }

  const workerPromise = runWorker();

  const server = createServer((req, res) => {
    if (req.method === "POST" && req.url === "/chat") {
      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
      });

      void (async () => {
        // 请求本身不再直接跑 mock 推理，而是把"要跑什么"交给 Redis Stream，
        // 由上面的 worker 循环取出来执行——对客户端来说接口和行为完全不变。
        const jobId = await redis.xadd(
          streamKey,
          "*",
          "tokens",
          JSON.stringify(tokens),
          "tokenDelayMs",
          String(tokenDelayMs),
        );
        if (!jobId) return;
        pendingResponses.set(jobId, res);
        onEvent({ type: "dispatched", jobId });
      })();
      return;
    }

    res.writeHead(404);
    res.end();
  });

  server.on("close", () => {
    stopped = true;
    void workerPromise.then(() => {
      redis.disconnect();
      consumerConnection.disconnect();
    });
  });

  return server;
}
