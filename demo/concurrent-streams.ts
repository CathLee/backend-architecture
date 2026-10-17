// 验证 ticket #7：大量并发流同时打开时，是否依然遵守并发上限。
// 2 个 worker + 最多排队 2 个 = 总容量 4，同时发起 12 个流式请求，
// 应该看到：同一时刻"正在被处理"的流从不超过 2 个，恰好 4 个被接受并完整完成，
// 其余 8 个立刻收到 429，而不是全部同时开始推流。
// 跑法：npm run demo:concurrent-streams
import { startGateway } from "../src/gateway.ts";

const PORT = 3005;
const WORKERS = 2;
const QUEUE = 2;
const REQUESTS = 12;

let inFlight = 0;
let maxInFlight = 0;

const server = startGateway({
  workerCount: WORKERS,
  maxQueueDepth: QUEUE,
  tokenDelayMs: 150,
  onEvent: (e) => {
    if (e.type === "claimed") {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      console.log(`[event] claimed     正在处理的流=${inFlight}  job=${e.jobId?.slice(0, 13)}`);
    } else if (e.type === "completed" || e.type === "failed") {
      inFlight -= 1;
      console.log(`[event] ${e.type.padEnd(11)} 正在处理的流=${inFlight}  job=${e.jobId?.slice(0, 13)}`);
    } else if (e.type === "rejected") {
      console.log("[event] rejected    <-- 容量已满，直接拒绝");
    }
  },
});

server.listen(PORT, async () => {
  console.log(
    `网关已启动：http://localhost:${PORT}（${WORKERS} 个 worker，最多排队 ${QUEUE} 个，总容量 ${WORKERS + QUEUE}）\n`,
  );
  console.log(`同时打开 ${REQUESTS} 个流式连接：\n`);

  const results = await Promise.all(
    Array.from({ length: REQUESTS }, async () => {
      const res = await fetch(`http://localhost:${PORT}/chat`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ prompt: "hi" }),
      });
      if (res.status === 429) return 429;
      await res.text();
      return 200;
    }),
  );

  const accepted = results.filter((s) => s === 200).length;
  const rejected = results.filter((s) => s === 429).length;
  console.log("\n结果汇总：");
  console.log(`  接受并完成(200): ${accepted} 个（期望 ${WORKERS + QUEUE}）`);
  console.log(`  拒绝(429): ${rejected} 个（期望 ${REQUESTS - WORKERS - QUEUE}）`);
  console.log(`  同一时刻最多在处理的流: ${maxInFlight} 个（期望 ≤ ${WORKERS}）`);

  server.close();
});
