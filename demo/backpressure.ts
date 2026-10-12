// 验证 ticket #6：持续超载时系统会不会被压垮。
// 容量 = workerCount(1) + maxQueueDepth(2) = 3，发 6 个并发请求，
// 应该看到恰好 3 个被接受、3 个立刻收到 429 + Retry-After，而不是 6 个全部排队等待
// （那样内存/延迟就会无限增长），也不是 6 个全部失败（服务被压垮）。
// 跑法：npm run demo:backpressure
import { startGateway } from "../src/gateway.ts";

const PORT = 3003;

const server = startGateway({
  workerCount: 1,
  maxQueueDepth: 2, // 总容量 = 1(worker) + 2(排队上限) = 3
  dispatchPoolSize: 3, // 派发连接池：3 条连接轮询分摊，而不是全部挤在 1 条连接上
  tokenDelayMs: 150,
  onEvent: (e) => {
    if (e.type === "rejected") {
      console.log("[event] rejected    <-- 容量已满，直接拒绝，不再往队列里塞");
    } else if (e.type === "dispatched") {
      console.log(
        `[event] dispatched  job=${e.jobId!.slice(0, 13)}  queueDepth=${e.queueDepth}`,
      );
    } else {
      console.log(`[event] ${e.type.padEnd(11)} job=${e.jobId?.slice(0, 13)}`);
    }
  },
});

server.listen(PORT, async () => {
  console.log(
    `网关已启动：http://localhost:${PORT}（1 个 worker，最多排队 2 个，总容量 3）\n`,
  );
  console.log("同时发起 6 个并发请求：\n");

  const results = await Promise.all(
    Array.from({ length: 6 }, async (_, i) => {
      const res = await fetch(`http://localhost:${PORT}/chat`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ prompt: "hi" }),
      });
      if (res.status === 429) {
        return { i, status: 429, retryAfter: res.headers.get("retry-after") };
      }
      await res.text(); // 读完流式响应，确认它真的完整完成了
      return { i, status: 200 };
    }),
  );

  console.log("\n结果汇总：");
  const accepted = results.filter((r) => r.status === 200);
  const rejected = results.filter((r) => r.status === 429);
  console.log(`  接受(200): ${accepted.length} 个`);
  console.log(
    `  拒绝(429): ${rejected.length} 个，Retry-After=${rejected[0]?.retryAfter}`,
  );
  console.log(
    "\n容量刚好是 3，所以应该是 3 个被接受、3 个被立刻拒绝——而不是 6 个全排队，也不是全失败。",
  );

  server.close();
});
