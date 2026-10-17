// 验证 ticket #9：上下文裁剪 + 会话亲和性路由。
// 1. 3 个 worker，3 个 session 各发 3 轮：同一个 session 的每一轮都落在同一个 worker 上。
// 2. maxHistoryTurns = 3，同一个 session 连发 5 轮后再问：只"记得"最近 3 轮。
// 3. 把某个 session 所在的 worker 杀掉：这个 session 换到别的 worker，历史照样在。
// 跑法：npm run demo:affinity
import Redis from "ioredis";
import { startGateway, type GatewayEvent } from "../src/gateway.ts";

const PORT = 3007;
let lastClaim: GatewayEvent | undefined;

const server = startGateway({
  workerCount: 3,
  maxHistoryTurns: 3,
  onEvent: (e) => {
    if (e.type === "claimed") lastClaim = e;
  },
});

async function chat(sessionId: string, prompt: string): Promise<string> {
  lastClaim = undefined;
  const res = await fetch(`http://127.0.0.1:${PORT}/chat`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ sessionId, prompt }),
  });
  return (await res.text())
    .split("\n\n")
    .filter((f) => f.startsWith("data: "))
    .map((f) => f.slice("data: ".length))
    .filter((d) => d !== "[DONE]")
    .join("");
}

server.listen(PORT, async () => {
  const stamp = Date.now();
  console.log("== 1. 会话亲和性：同一个 session 每一轮落在哪个 worker ==");
  for (const name of ["alice", "bob", "carol"]) {
    const sessionId = `demo-${name}-${stamp}`;
    const workers: string[] = [];
    for (let turn = 1; turn <= 3; turn += 1) {
      await chat(sessionId, `${name} turn ${turn}`);
      workers.push(lastClaim!.consumerName!);
    }
    console.log(`  ${name.padEnd(6)} -> ${workers.join(", ")}`);
  }

  console.log("\n== 2. 上下文裁剪：maxHistoryTurns = 3 ==");
  const trimmed = `demo-trim-${stamp}`;
  for (const word of ["alpha", "bravo", "charlie", "delta", "echo"]) {
    await chat(trimmed, word);
  }
  console.log(`  发了 alpha..echo 共 5 轮之后再问，回复：`);
  console.log(`  ${await chat(trimmed, "what do you remember?")}`);
  console.log("  ^ 期望只看到最近 3 轮（alpha、bravo 已被裁掉）");

  console.log("\n== 3. worker 下线：session 换 worker，历史不丢 ==");
  const moving = `demo-move-${stamp}`;
  await chat(moving, "remember-me");
  const before = lastClaim!.consumerName!;
  server.killWorker(Number(before.split("-")[1]));
  const reply = await chat(moving, "still there?");
  console.log(`  之前在 ${before}，杀掉它之后落在 ${lastClaim!.consumerName}`);
  console.log(`  回复：${reply}`);

  server.close();
  const redis = new Redis("redis://127.0.0.1:6379");
  const keys = await redis.keys(`session:demo-*-${stamp}:history`);
  if (keys.length > 0) await redis.del(...keys);
  redis.disconnect();
  setTimeout(() => process.exit(0), 500);
});
