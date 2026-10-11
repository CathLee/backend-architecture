import { createServer, type Server } from "node:http";

// MVP 阶段还没有接入真实的 AI 推理，这里用固定的 token 列表模拟"逐字吐字"的效果。
// 后续模块A会把这一步换成从 Redis Stream 里取任务再执行，但对外的 SSE 接口形状不变。
const MOCK_TOKENS = ["Hello", ", ", "world", "!"];

export interface StartGatewayOptions {
  // 每个 token 之间等待的毫秒数。设为 0（默认）时不等待，方便大多数测试跑得快；
  // 调大之后可以在测试里观察到"分块到达"而不是"一次性吐完"，也是后面复现队列堆积场景的开关。
  tokenDelayMs?: number;
  // 允许外部自定义要流式输出的 token 序列，而不是写死用 MOCK_TOKENS。
  tokens?: string[];
}

// 用 setTimeout 包一层 Promise，模拟真实推理里"等一会儿才有下一个 token"的延迟。
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function startGateway(options: StartGatewayOptions = {}): Server {
  const tokenDelayMs = options.tokenDelayMs ?? 0;
  const tokens = options.tokens ?? MOCK_TOKENS;

  return createServer((req, res) => {
    if (req.method === "POST" && req.url === "/chat") {
      // SSE（Server-Sent Events）要求这三个响应头：
      // - content-type 必须是 text/event-stream，浏览器/客户端靠这个识别这是一个事件流而不是普通响应体
      // - no-cache 防止中间代理或浏览器把流式响应缓存住
      // - keep-alive 保持连接不被提前关闭，因为后面还要分批 write
      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
      });

      // 用一个立即执行的 async 函数来逐个 write，中间插入可配置的延迟，
      // 这样客户端收到的就是多个分开的 chunk，而不是 Node 把所有内容攒成一个 chunk 一次性发出去。
      void (async () => {
        for (const token of tokens) {
          res.write(`data: ${token}\n\n`);
          if (tokenDelayMs > 0) await delay(tokenDelayMs);
        }
        // [DONE] 是约定的结束标记（很多 SSE 风格的 API，比如 OpenAI 的流式接口都用这个惯例），
        // 客户端看到这个事件就知道流结束了，不用等连接真正关闭才能判断。
        res.write("data: [DONE]\n\n");
        res.end();
      })();
      return;
    }

    // 这张票只实现 /chat 这一个路由，其它路径统一 404。
    res.writeHead(404);
    res.end();
  });
}
