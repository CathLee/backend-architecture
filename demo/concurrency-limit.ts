// 验证 ticket #5：只给 1 个 worker，同时发 3 个请求，
// 看后面两个请求是不是"排队等待"而不是被拒绝或者把服务搞挂——
// 而且这个排队深度（queueDepth）本身是可观察的。
// 跑法：npm run demo:concurrency
import { startGateway } from "../src/gateway.ts";

const PORT = 3002;

const server = startGateway({
  workerCount: 1, // 故意只给 1 个 worker，制造"容量明显不够"的场景
  tokenDelayMs: 300,
  onEvent: (e) => {
    if (e.type === "dispatched") {
      console.log(
        `[event] dispatched  job=${e.jobId.slice(0, 13)}  queueDepth=${e.queueDepth}` +
          (e.queueDepth! > 0 ? "  <-- 排队中，前面还有任务没处理完" : "  <-- 可以立刻处理"),
      );
    } else {
      console.log(`[event] ${e.type.padEnd(11)} job=${e.jobId.slice(0, 13)}`);
    }
  },
});

server.listen(PORT, async () => {
  console.log(`网关已启动：http://localhost:${PORT}（只有 1 个 worker）\n`);
  console.log("同时发起 3 个并发请求：\n");

  const labels = ["A", "B", "C"];
  const requests = labels.map(async (label) => {
    const res = await fetch(`http://localhost:${PORT}/chat`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ prompt: "hi" }),
    });
    const text = await res.text();
    const tokens = text
      .split("\n\n")
      .filter((c) => c.startsWith("data: "))
      .map((c) => c.slice("data: ".length))
      .join("");
    console.log(`\n>>> 请求 ${label} 完成，收到：${tokens}`);
  });

  await Promise.all(requests);

  console.log(
    "\n全部 3 个请求都成功完成了——只有 1 个 worker 也没有丢请求，只是排了个队。",
  );
  server.close();
});
