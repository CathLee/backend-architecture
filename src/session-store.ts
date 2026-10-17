import Redis from "ioredis";

export interface Turn {
  user: string;
  assistant: string;
}

// 会话历史有自己的连接和自己的 key 命名空间（session:<id>:history，一个 Redis list），
// 与队列用的 Stream 完全分开：队列里的消息处理完会被 ack、可以被裁掉，
// 而会话历史需要跨多次请求长期保留，两者的生命周期不同，不能混用同一份存储。
export class SessionStore {
  private readonly connection: Redis;

  constructor(redisUrl: string) {
    this.connection = new Redis(redisUrl);
  }

  private key(sessionId: string): string {
    return `session:${sessionId}:history`;
  }

  async getHistory(sessionId: string): Promise<Turn[]> {
    const raw = await this.connection.lrange(this.key(sessionId), 0, -1);
    return raw.map((entry) => JSON.parse(entry) as Turn);
  }

  async appendTurn(sessionId: string, turn: Turn): Promise<void> {
    await this.connection.rpush(this.key(sessionId), JSON.stringify(turn));
  }

  disconnect(): void {
    this.connection.disconnect();
  }
}
