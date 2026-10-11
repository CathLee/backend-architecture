import { describe, it, expect, afterEach } from "vitest";
import { startGateway } from "../src/gateway.ts";
import type { GatewayServer } from "../src/gateway.ts";

function listen(server: GatewayServer): Promise<number> {
  return new Promise((resolve) => {
    server.listen(0, () => {
      const address = server.address();
      if (address && typeof address === "object") {
        resolve(address.port);
      }
    });
  });
}

function parseSseEvents(raw: string): string[] {
  return raw
    .split("\n\n")
    .filter((chunk) => chunk.startsWith("data: "))
    .map((chunk) => chunk.slice("data: ".length));
}

// 事件是异步产生的（消费者要先从 Redis 读到消息才会触发），
// 这里简单轮询等到某个条件满足为止，而不是瞎猜一个固定延迟。
async function waitFor<T>(
  check: () => T | undefined,
  timeoutMs = 2000,
): Promise<T> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const result = check();
    if (result) return result;
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error("waitFor timed out");
}

// 这组测试要证明"加固"真正做到了什么：一个 worker 在 ack 之前被"杀掉"，
// 任务不会丢，会被同一个 Consumer Group 里的另一个 worker 接手并最终完成，
// 客户端最终仍然拿到完整结果，而且任务只被完整处理了一次（没有重复推送）。
describe("gateway consumer group ack/retry", () => {
  let server: GatewayServer | undefined;

  afterEach(() => {
    server?.close();
  });

  it("redelivers a job to a different worker and still completes the client's request when the original worker crashes before acking", async () => {
    const events: Array<{ type: string; jobId: string; consumerName?: string }> =
      [];

    server = startGateway({
      workerCount: 2,
      consumerStaleAfterMs: 100,
      claimToProcessGraceMs: 80,
      tokenDelayMs: 20,
      onEvent: (e) => events.push(e),
    }) as GatewayServer;
    const port = await listen(server);

    const resPromise = fetch(`http://localhost:${port}/chat`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ prompt: "hi" }),
    });

    // 等到某个 worker 真正 claim 到这个任务，再精确地把那一个 worker 杀掉
    // （不是随便猜哪个 worker 会抢到，而是先观察到事实再杀对的那个）。
    const claimed = await waitFor(() =>
      events.find((e) => e.type === "claimed"),
    );
    const crashedConsumer = claimed.consumerName!;
    const workerIndex = Number(crashedConsumer.split("-")[1]);
    server.killWorker(workerIndex);

    const res = await resPromise;
    const parsedEvents = parseSseEvents(await res.text());
    expect(parsedEvents).toEqual(["Hello", ", ", "world", "!", "[DONE]"]);

    const redelivered = events.find((e) => e.type === "redelivered");
    expect(redelivered).toBeTruthy();
    // 必须是不同的 worker 接手，证明确实发生了"重新投递"，不是原 worker 自己完成的。
    expect(redelivered!.consumerName).not.toBe(crashedConsumer);

    // 客户端的 res.end() 和服务端"记一条 completed 事件"是两件分开的事：
    // res.end() 一发生，fetch 那边就能拿到完整响应，但 xack + onEvent("completed")
    // 是在那之后才跑的异步收尾，所以不能一读完响应就断言 completed 事件已经存在，
    // 还要再等它一下。
    await waitFor(() => events.find((e) => e.type === "completed"));

    // 只应该被完整处理一次：crash 的那个 worker 没有 ack，所以它不该算完成；
    // 新 worker 完成后 ack 了，所以总共只有一条 completed 事件。
    const completed = events.filter((e) => e.type === "completed");
    expect(completed).toHaveLength(1);
    expect(completed[0].consumerName).toBe(redelivered!.consumerName);
  });
});
