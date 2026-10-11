// 最基础的手动验证：启动网关，发一个真实的 HTTP 请求，
// 实时打印收到的每一个 SSE 事件和最终拼出来的流式文本。
// 跑法：npm run demo:basic
import { startGateway } from "../src/gateway.ts";

const PORT = 3000;

const server = startGateway({
  tokenDelayMs: 300, // 故意放慢，方便肉眼看到"一个字一个字"到达的效果
  onEvent: (e) => {
    console.log(`[event] ${e.type.padEnd(11)} job=${e.jobId.slice(0, 13)}`);
  },
});

server.listen(PORT, async () => {
  console.log(`网关已启动：http://localhost:${PORT}`);
  console.log("发起 POST /chat ...\n");

  const res = await fetch(`http://localhost:${PORT}/chat`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ prompt: "hi" }),
  });

  console.log(`响应头 content-type: ${res.headers.get("content-type")}\n`);

  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  process.stdout.write("流式响应：");

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

  console.log("\n\n完成，关闭网关。");
  server.close();
});
