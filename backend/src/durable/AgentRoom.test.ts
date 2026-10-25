import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("cloudflare:workers", () => ({
  DurableObject: class {
    constructor(
      protected readonly ctx: DurableObjectState,
      protected readonly env: Cloudflare.Env
    ) {}
  },
}));

import { AgentRoom } from "./AgentRoom";

class SocketStub {
  readonly send = vi.fn<(message: string) => void>();

  constructor(private readonly attachment: unknown) {}

  deserializeAttachment() {
    return this.attachment;
  }
}

function subscriber(agentIds: number[], scope: "admin" | "public") {
  return new SocketStub({ kind: "subscriber", agentIds, scope, connectedAt: 1 });
}

function createRoom(sockets: SocketStub[]) {
  const context = {
    getWebSockets: () => sockets,
    setWebSocketAutoResponse: vi.fn(),
  };
  return new AgentRoom(context as unknown as DurableObjectState, {} as Cloudflare.Env);
}

function push(room: AgentRoom, agentId: number, ts = 1) {
  return room.fetch(new Request("http://internal/push", {
    method: "POST",
    body: JSON.stringify({
      agentId,
      samples: [{
        ts,
        data: { cpu_usage: ts, cpu_model: "private-model" },
        publicData: { cpu_usage: ts },
      }],
    }),
  }));
}

function lastMessage(socket: SocketStub) {
  const message = socket.send.mock.calls.at(-1)?.[0];
  if (!message) throw new Error("subscriber received no update");
  return JSON.parse(message) as {
    updates: Array<{ agentId: number; samples: Array<{ data: Record<string, unknown> }> }>;
  };
}

beforeEach(() => {
  vi.stubGlobal("WebSocketRequestResponsePair", class {
    constructor(readonly request: string, readonly response: string) {}
  });
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("AgentRoom broadcast serialization", () => {
  it("每种 scope 只序列化一次，且只发送给订阅当前 Agent 的连接", async () => {
    const firstAdmin = subscriber([1], "admin");
    const secondAdmin = subscriber([1, 2], "admin");
    const firstPublic = subscriber([1], "public");
    const secondPublic = subscriber([1, 3], "public");
    const unrelated = subscriber([2], "admin");
    const upstream = new SocketStub({ kind: "agent", agentId: 1, connectedAt: 1, sequence: 0 });
    const room = createRoom([
      firstAdmin, secondAdmin, firstPublic, secondPublic, unrelated, upstream,
    ]);
    const stringify = vi.spyOn(JSON, "stringify");

    const response = await push(room, 1);

    expect(await response.json()).toEqual({ ok: true, delivered: 4 });
    expect(stringify.mock.calls.filter(([value]) =>
      value && typeof value === "object" && value.type === "batchUpdate"
    )).toHaveLength(2);
    expect(firstAdmin.send.mock.calls).toEqual(secondAdmin.send.mock.calls);
    expect(firstPublic.send.mock.calls).toEqual(secondPublic.send.mock.calls);
    expect(lastMessage(firstAdmin).updates[0].samples[0].data.cpu_model)
      .toBe("private-model");
    expect(lastMessage(firstPublic).updates[0].samples[0].data)
      .toEqual({ cpu_usage: 1 });
    expect(unrelated.send).not.toHaveBeenCalled();
    expect(upstream.send).not.toHaveBeenCalled();
  });

  it("不同 Agent 和后续更新不共用上次缓存的消息", async () => {
    const firstOnly = subscriber([1], "admin");
    const both = subscriber([1, 2], "admin");
    const secondPublic = subscriber([2], "public");
    const room = createRoom([firstOnly, both, secondPublic]);

    await push(room, 1, 10);
    await push(room, 2, 20);
    await push(room, 2, 30);

    expect(firstOnly.send).toHaveBeenCalledTimes(1);
    expect(both.send).toHaveBeenCalledTimes(3);
    expect(secondPublic.send).toHaveBeenCalledTimes(2);
    expect(lastMessage(firstOnly).updates[0].agentId).toBe(1);
    expect(lastMessage(both).updates[0].agentId).toBe(2);
    expect(lastMessage(secondPublic).updates[0]).toEqual({
      agentId: 2,
      samples: [{ ts: 30, data: { cpu_usage: 30 } }],
    });
  });

  it("单个连接发送失败不会阻断同 scope 的其他订阅者", async () => {
    const broken = subscriber([1], "public");
    const healthy = subscriber([1], "public");
    broken.send.mockImplementationOnce(() => { throw new Error("socket closed"); });
    const room = createRoom([broken, healthy]);

    const response = await push(room, 1);

    expect(await response.json()).toEqual({ ok: true, delivered: 1 });
    expect(healthy.send).toHaveBeenCalledOnce();
    expect(lastMessage(healthy).updates[0].samples[0].data)
      .toEqual({ cpu_usage: 1 });
  });
});
