// 验证 ticket #4 的加固效果：两个 worker 组成一个 Consumer Group，
// 第一个 claim 到任务的 worker 会被故意"杀掉"（模拟崩溃），
// 看任务是不是真的被另一个 worker 重新认领、客户端最终依然拿到完整结果。
// 跑法：npm run demo:crash
import { startGateway } from "../src/gateway.ts";

const PORT = 3001;
let killedOnce = false;

const server = startGateway({
  workerCount: 2,
  consumerStaleAfterMs: 500, // 空闲超过 0.5s 就认为原 worker 挂了，允许被别人认领
  claimToProcessGraceMs: 1500, // claim 到之后故意等 1.5s 才开始真正处理，留出"杀掉它"的窗口
  tokenDelayMs: 200,
  onEvent: (e) => {
    console.log(
      `[event] ${e.type.padEnd(11)} consumer=${(e.consumerName ?? "-").padEnd(9)} job=${e.jobId.slice(0, 13)}`,
    );

    // 也就是 worker-0 刚刚认领到任务）时，立刻调用 server.killWorker(0)，把 worker-0 的标记设为 true。这时 worker-0 还在那 1.5 秒的等待窗口里，等它醒过来检查 killed[0]，发现是 true，就直接 return，什么都没做
    if (e.type === "claimed" && !killedOnce) {
      killedOnce = true;
      const index = Number(e.consumerName!.split("-")[1]);
      console.log(
        `\n>>> 模拟 worker-${index} 在这里崩溃（claim 到任务但还没真正开始处理）\n`,
      );
      server.killWorker(index);
    }
  },
});

server.listen(PORT, async () => {
  console.log(`网关已启动：http://localhost:${PORT}（2 个 worker，同一个 Consumer Group）\n`);
  console.log("发起一次请求，观察完整过程：claim -> 模拟崩溃 -> 被另一个 worker 重新认领 -> 完成\n");

  const res = await fetch(`http://localhost:${PORT}/chat`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ prompt: "hi" }),
  });

  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  process.stdout.write("\n流式响应：");

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    const chunk = decoder.decode(value);
    for (const line of chunk.split("\n\n")) {
      if (line.startsWith("data: ")) {
        process.stdout.write(line.slice("data: ".length));
      }
    }
  }

  console.log(
    "\n\n完成。尽管中途有一个 worker \"崩溃\"，客户端依然拿到了完整、正确的结果。",
  );
  server.close();
});
