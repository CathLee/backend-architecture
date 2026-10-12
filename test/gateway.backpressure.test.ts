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

// 这组测试证明：持续超载时，系统不会无限制地把请求都塞进队列（那样内存/延迟会无限增长），
// 而是在容量（workerCount + maxQueueDepth）用满之后，直接拒绝多余的请求并给出明确信号
// （429 + Retry-After），不是让它们无限排队，也不是把服务压垮。
describe("gateway backpressure", () => {
  let server: GatewayServer | undefined;

  afterEach(() => {
    server?.close();
  });

  it("accepts requests up to capacity and immediately rejects the rest with 429 + Retry-After, instead of queueing them indefinitely", async () => {
    // 容量 = workerCount(1) + maxQueueDepth(1) = 2：最多 2 个请求能被接受。
    server = startGateway({
      workerCount: 1,
      maxQueueDepth: 1,
      tokenDelayMs: 50,
    }) as GatewayServer;
    const port = await listen(server);

    const responses = await Promise.all(
      Array.from({ length: 4 }, () =>
        fetch(`http://localhost:${port}/chat`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ prompt: "hi" }),
        }),
      ),
    );

    const accepted = responses.filter((r) => r.status === 200);
    const rejected = responses.filter((r) => r.status === 429);

    // 容量只有 2，4 个并发请求里必须恰好 2 个被接受、2 个被拒绝——
    // 不能变成"全部排队等待"（那样就没有背压了），也不能变成"全部失败"（服务被压垮了）。
    expect(accepted).toHaveLength(2);
    expect(rejected).toHaveLength(2);

    // 被拒绝的请求要有明确、可操作的信号，而不是裸的失败。
    for (const res of rejected) {
      expect(res.headers.get("retry-after")).toBeTruthy();
    }

    // 被接受的请求依然要能拿到完整、正确的流式结果。
    for (const res of accepted) {
      const events = parseSseEvents(await res.text());
      expect(events).toEqual(["Hello", ", ", "world", "!", "[DONE]"]);
    }
  });
});
