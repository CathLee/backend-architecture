import { describe, it, expect, afterEach } from "vitest";
import { startGateway } from "../src/gateway.ts";
import type { GatewayServer } from "../src/gateway.ts";

import { listen } from "./listen.ts";

function parseSseEvents(raw: string): string[] {
  return raw
    .split("\n\n")
    .filter((chunk) => chunk.startsWith("data: "))
    .map((chunk) => chunk.slice("data: ".length));
}

// 这组测试证明：并发请求数超过 worker 池容量时，多出来的请求会排队等待，
// 而不是被拒绝、丢弃或把服务搞崩——而且这个排队现象本身是可观察的
// （通过 dispatched 事件里的 queueDepth，而不是去戳 Redis Stream 的长度）。
describe("gateway concurrency limits", () => {
  let server: GatewayServer | undefined;

  afterEach(() => {
    server?.close();
  });

  it("queues excess concurrent requests instead of rejecting them, and makes the queue depth observable", async () => {
    const dispatchedEvents: Array<{ queueDepth?: number }> = [];

    // 只配 1 个 worker，容量很小，3 个并发请求必然会有 2 个要排队。
    server = startGateway({
      workerCount: 1,
      tokenDelayMs: 30,
      onEvent: (e) => {
        if (e.type === "dispatched") dispatchedEvents.push(e);
      },
    }) as GatewayServer;
    const port = await listen(server);

    const requests = Array.from({ length: 3 }, () =>
      fetch(`http://localhost:${port}/chat`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ prompt: "hi" }),
      }),
    );

    const responses = await Promise.all(requests);

    // 全部请求最终都要成功完成，拿到完整、正确的结果——没有被拒绝、没有丢失。
    for (const res of responses) {
      const events = parseSseEvents(await res.text());
      expect(events).toEqual(["Hello", ", ", "world", "!", "[DONE]"]);
    }

    // 3 个并发请求、只有 1 个 worker，必然有请求在 dispatched 时就已经排在队列里
    // （前面还有别的任务没处理完），这里断言这个排队状态确实被观察到了。
    const queueDepths = dispatchedEvents.map((e) => e.queueDepth ?? 0);
    expect(Math.max(...queueDepths)).toBeGreaterThan(0);
  });
});
