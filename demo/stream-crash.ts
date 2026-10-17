// 验证 ticket #7：worker 在推流"中途"挂掉时，客户端会不会被挂死。
// 第一个 claim 到任务的 worker 会在吐出几个 token 之后被杀掉，
// 应该看到客户端收到一个明确的 `event: error`（retryable），流随之结束，
// 没有 [DONE]、没有重复 token，而不是无限挂起。
// 跑法：npm run demo:stream-crash
import { startGateway } from "../src/gateway.ts";

const PORT = 3004;
let killedOnce = false;

const server = startGateway({
  workerCount: 2,
  consumerStaleAfterMs: 500,
  tokenDelayMs: 200,
  tokens: Array.from({ length: 10 }, (_, i) => `t${i} `),
  onEvent: (e) => {
    console.log(
      `[event] ${e.type.padEnd(11)} consumer=${(e.consumerName ?? "-").padEnd(9)} job=${e.jobId?.slice(0, 13)}`,
    );

    if (e.type === "claimed" && !killedOnce) {
      killedOnce = true;
      const index = Number(e.consumerName!.split("-")[1]);
      // 等它吐出几个 token 之后再杀，模拟"推流中途"而不是"开始之前"。
      setTimeout(() => {
        console.log(`\n>>> 模拟 worker-${index} 在推流中途崩溃\n`);
        server.killWorker(index);
      }, 700);
    }
  },
});

server.listen(PORT, async () => {
  console.log(`网关已启动：http://localhost:${PORT}（2 个 worker）\n`);
  console.log("发起一次请求，观察：吐出几个 token -> worker 崩溃 -> 客户端收到 error 帧\n");

  const res = await fetch(`http://localhost:${PORT}/chat`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ prompt: "hi" }),
  });

  // 加超时：如果流挂死（没有 end/error 信号），要明确报出来，而不是让 demo 卡住。
  const timeout = setTimeout(() => {
    console.log("\n[FAIL] 5 秒内流既没结束也没收到 error —— 客户端被挂死了");
    process.exit(1);
  }, 5000);

  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  let sawError = false;
  let sawDone = false;
  process.stdout.write("流式响应：");

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    for (const frame of decoder.decode(value).split("\n\n")) {
      if (frame.startsWith("event: error")) {
        sawError = true;
        process.stdout.write(`\n收到错误帧 -> ${frame.split("\n")[1]}`);
      } else if (frame.startsWith("data: [DONE]")) {
        sawDone = true;
      } else if (frame.startsWith("data: ")) {
        process.stdout.write(frame.slice("data: ".length));
      }
    }
  }
  clearTimeout(timeout);

  console.log(
    `\n\n结果：error 帧=${sawError}，[DONE]=${sawDone}。` +
      "\n期望 error 帧=true、[DONE]=false：客户端明确知道这次失败了、可以重试，没有被挂死。",
  );
  server.close();
});
