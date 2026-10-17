// 验证 ticket #8：同一个 session 的后续请求能不能"记得"之前说过的话。
// 同一个 sessionId 连发三轮，每一轮的 mock 回复都应该带上之前几轮的内容；
// 换一个 sessionId 则看不到这些历史。同时直接去 Redis 里看：
// 会话历史是独立的 list（session:<id>:history），和队列用的 Stream 是两个不同的 key。
// 跑法：npm run demo:session
import Redis from "ioredis";
import { startGateway } from "../src/gateway.ts";

const PORT = 3006;
const streamKey = "demo:session:jobs";
const sessionId = `demo-${Date.now()}`;
const otherSessionId = `demo-${Date.now()}-other`;

async function chat(id: string, prompt: string): Promise<string> {
  const res = await fetch(`http://127.0.0.1:${PORT}/chat`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ sessionId: id, prompt }),
  });
  const frames = (await res.text())
    .split("\n\n")
    .filter((f) => f.startsWith("data: "))
    .map((f) => f.slice("data: ".length))
    .filter((d) => d !== "[DONE]");
  return frames.join("");
}

const server = startGateway({ streamKey });

server.listen(PORT, async () => {
  console.log(`网关已启动：http://localhost:${PORT}\n`);

  for (const prompt of ["my name is Alice", "I like Redis", "what do you remember?"]) {
    console.log(`[${sessionId.slice(-6)}] 用户: ${prompt}`);
    console.log(`[${sessionId.slice(-6)}] 回复: ${await chat(sessionId, prompt)}\n`);
  }

  console.log(`换一个全新的 session（${otherSessionId.slice(-6)}）问同样的话：`);
  console.log(`回复: ${await chat(otherSessionId, "what do you remember?")}`);
  console.log("  ^ 期望 \"No earlier context.\"：不同 session 之间互相看不到历史\n");

  const redis = new Redis("redis://127.0.0.1:6379");
  console.log("直接看 Redis 里的存储：");
  console.log(
    `  session:${sessionId}:history  type=${await redis.type(`session:${sessionId}:history`)}  长度=${await redis.llen(`session:${sessionId}:history`)}`,
  );
  console.log(`  ${streamKey}:0  type=${await redis.type(`${streamKey}:0`)}`);
  console.log("  ^ 会话历史是独立的 list，和队列的 stream 是两个不同的 key，互不借用存储\n");

  // 先关网关，等 worker 的最后一轮 XREADGROUP（最长阻塞 200ms）结束，再清理 key；
  // 否则 worker 还在读的时候 stream 被删掉，会报 NOGROUP。
  server.close();
  await new Promise((r) => setTimeout(r, 500));
  await redis.del(
    `session:${sessionId}:history`,
    `session:${otherSessionId}:history`,
    `${streamKey}:0`,
  );
  redis.disconnect();
});
