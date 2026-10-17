import { describe, it, expect, afterEach } from "vitest";
import { startGateway } from "../src/gateway.ts";
import type { GatewayEvent, GatewayServer } from "../src/gateway.ts";

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

function parseSseFrames(raw: string): Array<{ event?: string; data: string }> {
  return raw
    .split("\n\n")
    .filter((chunk) => chunk.trim().length > 0)
    .map((chunk) => {
      const frame: { event?: string; data: string } = { data: "" };
      for (const line of chunk.split("\n")) {
        if (line.startsWith("event: ")) frame.event = line.slice("event: ".length);
        if (line.startsWith("data: ")) frame.data = line.slice("data: ".length);
      }
      return frame;
    });
}

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

// "挂死"是这类 bug 的真实表现形式：客户端的 text() 永远不 resolve。
// 所以必须给读取加一个超时，把"挂死"变成一个明确的测试失败，而不是让整个测试卡到全局超时。
function readWithin(res: Response, ms: number): Promise<string> {
  return Promise.race([
    res.text(),
    new Promise<string>((_, reject) =>
      setTimeout(() => reject(new Error("stream hung: no end/error signal")), ms),
    ),
  ]);
}

describe("gateway stream resilience", () => {
  let server: GatewayServer | undefined;

  afterEach(() => {
    server?.close();
  });

  it("when a worker dies mid-stream, the client gets an explicit retryable error frame and the stream ends instead of hanging", async () => {
    const events: GatewayEvent[] = [];
    const tokens = Array.from({ length: 10 }, (_, i) => `t${i}`);

    server = startGateway({
      workerCount: 2,
      consumerStaleAfterMs: 100,
      tokenDelayMs: 40,
      tokens,
      onEvent: (e) => events.push(e),
    }) as GatewayServer;
    const port = await listen(server);

    const resPromise = fetch(`http://localhost:${port}/chat`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ prompt: "hi" }),
    });

    const claimed = await waitFor(() => events.find((e) => e.type === "claimed"));
    // 等它真正开始推流（已经吐出了几个 token）再杀，模拟"推流中途"而不是"开始前"。
    await new Promise((r) => setTimeout(r, 120));
    server.killWorker(Number(claimed.consumerName!.split("-")[1]));

    const res = await resPromise;
    const frames = parseSseFrames(await readWithin(res, 3000));

    const errorFrame = frames.find((f) => f.event === "error");
    expect(errorFrame).toBeTruthy();
    expect(JSON.parse(errorFrame!.data)).toMatchObject({
      error: "worker_failed",
      retryable: true,
    });

    // 中途失败的流不能带 [DONE]，否则客户端会误以为拿到了完整结果。
    expect(frames.some((f) => f.data === "[DONE]")).toBe(false);

    // 已经推出去的 token 不能重复：不能是"半截 + 另一个 worker 又完整重推一遍"。
    const received = frames.filter((f) => !f.event).map((f) => f.data);
    expect(received.length).toBeGreaterThan(0);
    expect(received.length).toBeLessThan(tokens.length);
    expect(received).toEqual(tokens.slice(0, received.length));

    // 这个任务已经向客户端交代过结果了（失败也是一种结果），不能再被别的 worker 重新认领——
    // 否则它会一直留在 pending 列表里被反复重投递，永远不会消失。
    await new Promise((r) => setTimeout(r, 400));
    expect(events.filter((e) => e.type === "redelivered")).toHaveLength(0);
    expect(events.filter((e) => e.type === "failed")).toHaveLength(1);
  });

  it("a healthy worker whose stream outlasts consumerStaleAfterMs keeps its job: no redelivery, no duplicated tokens", async () => {
    const events: GatewayEvent[] = [];
    const tokens = Array.from({ length: 8 }, (_, i) => `t${i}`);

    // 流总时长约 8 * 60 = 480ms，远超 staleAfterMs(100ms)，但 worker 一直活着。
    server = startGateway({
      workerCount: 2,
      consumerStaleAfterMs: 100,
      tokenDelayMs: 60,
      tokens,
      onEvent: (e) => events.push(e),
    }) as GatewayServer;
    const port = await listen(server);

    const res = await fetch(`http://localhost:${port}/chat`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ prompt: "hi" }),
    });
    const frames = parseSseFrames(await readWithin(res, 3000));

    expect(frames.map((f) => f.data)).toEqual([...tokens, "[DONE]"]);
    await waitFor(() => events.find((e) => e.type === "completed"));
    expect(events.filter((e) => e.type === "redelivered")).toHaveLength(0);
    expect(events.filter((e) => e.type === "completed")).toHaveLength(1);
  });

  it("many concurrently open streams never exceed workerCount in flight, and everything beyond workerCount + maxQueueDepth is rejected", async () => {
    let inFlight = 0;
    let maxInFlight = 0;
    const workerCount = 2;
    const maxQueueDepth = 2;

    server = startGateway({
      workerCount,
      maxQueueDepth,
      tokenDelayMs: 20,
      onEvent: (e) => {
        if (e.type === "claimed") {
          inFlight += 1;
          maxInFlight = Math.max(maxInFlight, inFlight);
        }
        if (e.type === "completed" || e.type === "failed") inFlight -= 1;
      },
    }) as GatewayServer;
    const port = await listen(server);

    const responses = await Promise.all(
      Array.from({ length: 20 }, () =>
        fetch(`http://localhost:${port}/chat`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ prompt: "hi" }),
        }),
      ),
    );

    const accepted = responses.filter((r) => r.status === 200);
    const rejected = responses.filter((r) => r.status === 429);
    expect(accepted).toHaveLength(workerCount + maxQueueDepth);
    expect(rejected).toHaveLength(20 - (workerCount + maxQueueDepth));

    for (const res of accepted) {
      const frames = parseSseFrames(await readWithin(res, 5000));
      expect(frames.map((f) => f.data)).toEqual([
        "Hello",
        ", ",
        "world",
        "!",
        "[DONE]",
      ]);
    }

    expect(maxInFlight).toBeLessThanOrEqual(workerCount);
    expect(maxInFlight).toBeGreaterThan(0);
  });
});
