// MCP サーバー（supabase/functions/agri-mcp）のテスト。実行: node --test tests/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createDataSource } from "../supabase/functions/agri-mcp/data.js";
import { createMcpHandler } from "../supabase/functions/agri-mcp/mcp.js";
import { computePhase, periodRange, rollingSeries } from "../supabase/functions/agri-mcp/analytics.js";

function makeRows(days = 120) {
  const rows = [];
  const start = Date.UTC(2026, 5, 1);
  for (let d = 0; d < days; d += 1) {
    const dt = new Date(start + d * 864e5);
    if (dt.getUTCDay() === 0) continue;
    const date = dt.toISOString().slice(0, 10);
    const w = (2 * Math.PI * d) / 42;
    rows.push({ sale_date: date, item_name: "レタス", quantity: 800 * (1 + 0.4 * Math.sin(w)), avg_price: 300 * (1 - 0.3 * Math.sin(w - 0.8)), high_price: 500, low_price: 100 });
    rows.push({ sale_date: date, item_name: "島バナナ", quantity: 60, avg_price: 700, high_price: 900, low_price: 400 });
  }
  return rows;
}

function mockFetch(rows, log) {
  return async (url, init) => {
    log.push(init.headers.Range);
    const [from, to] = init.headers.Range.split("-").map(Number);
    return new Response(JSON.stringify(rows.slice(from, to + 1)), { status: 200 });
  };
}

function setup(rows = makeRows()) {
  const log = [];
  const data = createDataSource({ supabaseUrl: "https://example.supabase.co", anonKey: "anon", fetchImpl: mockFetch(rows, log) });
  const handle = createMcpHandler({ getRows: () => data.getRows() });
  let id = 0;
  const rpc = async (method, params) => {
    const res = await handle(new Request("http://x/functions/v1/agri-mcp", { method: "POST", body: JSON.stringify({ jsonrpc: "2.0", id: ++id, method, params }) }));
    return res.json();
  };
  const call = async (name, args) => (await rpc("tools/call", { name, arguments: args })).result;
  return { handle, rpc, call, log };
}

test("initialize negotiates protocol version and returns instructions", async () => {
  const { rpc } = setup();
  const r = await rpc("initialize", { protocolVersion: "2025-03-26", capabilities: {}, clientInfo: { name: "t", version: "1" } });
  assert.equal(r.result.protocolVersion, "2025-03-26");
  assert.ok(r.result.capabilities.tools);
  assert.match(r.result.instructions, /販売価格中値/);
  const [icon] = r.result.serverInfo.icons;
  assert.equal(icon.mimeType, "image/png");
  assert.match(icon.src, /^data:image\/png;base64,[A-Za-z0-9+/]+=*$/);
  const unknown = await rpc("initialize", { protocolVersion: "1999-01-01" });
  assert.equal(unknown.result.protocolVersion, "2025-06-18");
});

test("notifications get 202, GET gets 405, unknown method is -32601, batches work", async () => {
  const { handle, rpc } = setup();
  const n = await handle(new Request("http://x", { method: "POST", body: JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }) }));
  assert.equal(n.status, 202);
  assert.equal((await handle(new Request("http://x", { method: "GET" }))).status, 405);
  assert.equal((await rpc("no/such")).error.code, -32601);
  const batch = await handle(new Request("http://x", { method: "POST", body: JSON.stringify([{ jsonrpc: "2.0", id: 1, method: "ping" }, { jsonrpc: "2.0", method: "notifications/initialized" }]) }));
  assert.deepEqual(await batch.json(), [{ jsonrpc: "2.0", id: 1, result: {} }]);
  const bad = await handle(new Request("http://x", { method: "POST", body: "{" }));
  assert.equal((await bad.json()).error.code, -32700);
});

test("tools/list exposes read-only tools without implementation details", async () => {
  const { rpc } = setup();
  const { tools } = (await rpc("tools/list")).result;
  assert.deepEqual(tools.map((t) => t.name), ["list_items", "get_item_history", "get_price_ranking", "get_weekly_change", "get_phase_state"]);
  tools.forEach((t) => {
    assert.equal(t.run, undefined);
    assert.equal(t.inputSchema.type, "object");
    assert.equal(t.annotations.readOnlyHint, true);
  });
});

test("item names match by hiragana reading and report errors as tool errors", async () => {
  const { call } = setup();
  const ok = await call("get_item_history", { item: "しまばなな", days: 7 });
  assert.equal(ok.isError, undefined);
  assert.equal(JSON.parse(ok.content[0].text).item, "島バナナ");
  const missing = await call("get_item_history", { item: "キャベツ" });
  assert.equal(missing.isError, true);
  const badArg = await call("get_price_ranking", { top: 99 });
  assert.equal(badArg.isError, true);
  assert.equal((await setup().rpc("tools/call", { name: "nope", arguments: {} })).error.code, -32602);
});

test("data source pages through REST results and caches them", async () => {
  const rows = [];
  for (let i = 0; i < 2500; i += 1) rows.push({ sale_date: "2026-09-01", item_name: `品目${i}`, quantity: 1, avg_price: 1 });
  const { call, log } = setup(rows);
  await call("list_items", {});
  await call("list_items", { query: "品目1" });
  assert.deepEqual(log, ["0-999", "1000-1999", "2000-2999"]);
});

test("phase state labels the cycle and reports the actual time of the point", async () => {
  const { call } = setup();
  const res = JSON.parse((await call("get_phase_state", { items: ["レタス"], period_days: 90 })).content[0].text);
  const item = res.items[0];
  assert.match(item.latest_point_as_of, /頃$/);
  assert.ok(["高値圏で上昇中", "高値圏から下落", "安値圏で下落中", "安値圏から回復"].includes(item.price.phase));
  assert.ok(item.quantity.price_pressure);
  // 最新の点は、データ最新日の7〜8日前
  const latest = res.latest_data_date;
  assert.equal(item.latest_point_as_of.slice(0, 10), new Date(Date.parse(`${latest}T00:00:00Z`) - 8 * 864e5).toISOString().slice(0, 10));
});

test("two-stage moving average has weights 1,2,3,3,3,3,3,2,1 (/21)", () => {
  const series = [];
  for (let d = 0; d < 40; d += 1) {
    const date = new Date(Date.UTC(2026, 5, 1) + d * 864e5).toISOString().slice(0, 10);
    series.push({ sale_date: date, quantity: d === 20 ? 1100 : 100, avg_price: 100 });
  }
  const r = rollingSeries(series, "2026-06-15", "2026-07-01");
  const weights = r.map((x) => Math.round(((x.q - 100) * 21) / 1000));
  assert.deepEqual(weights.filter(Boolean), [1, 2, 3, 3, 3, 3, 3, 2, 1]);
  assert.ok(computePhase(series, periodRange("2026-07-10", "2026-06-01", 30), "p"));
});

// ---- 利用状況ログ ----
import { createUsageLogger, clientLabel, MCP_VISITOR_ID } from "../supabase/functions/agri-mcp/usage.js";

function setupWithLog() {
  const events = [];
  const pending = [];
  const data = createDataSource({ supabaseUrl: "https://example.supabase.co", anonKey: "anon", fetchImpl: mockFetch(makeRows(), []) });
  const handle = createMcpHandler({
    getRows: () => data.getRows(),
    logEvent: (e) => events.push(e),
    waitUntil: (p) => pending.push(p),
  });
  const post = (body, ua = "Claude-User") =>
    handle(new Request("http://x", { method: "POST", headers: { "user-agent": ua }, body: JSON.stringify(body) }));
  return { events, pending, post };
}

test("logs connects and tool calls with resolved item names, args and errors", async () => {
  const { events, pending, post } = setupWithLog();
  await post({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-06-18", clientInfo: { name: "claude-ai", version: "1.0" } } });
  await post({ jsonrpc: "2.0", id: 2, method: "tools/call", params: { name: "get_phase_state", arguments: { items: ["しまばなな", "レタス"], period_days: 30 } } });
  await post({ jsonrpc: "2.0", id: 3, method: "tools/call", params: { name: "get_item_history", arguments: { item: "キャベツ" } } }, "openai-mcp/1.0");
  await post({ jsonrpc: "2.0", id: 4, method: "tools/call", params: { name: "nope", arguments: {} } });
  await post({ jsonrpc: "2.0", id: 5, method: "tools/list" });
  assert.equal(pending.length, 4, "every log goes through waitUntil");
  const [connect, phase, missing, unknown] = events;
  assert.equal(connect.eventType, "mcp_connect");
  assert.equal(connect.metadata.client, "Claude");
  assert.equal(connect.metadata.client_name, "claude-ai");
  assert.equal(phase.eventType, "mcp_call");
  assert.equal(phase.pagePath, "/mcp/get_phase_state");
  assert.deepEqual(phase.metadata.items.sort(), ["レタス", "島バナナ"].sort());
  assert.deepEqual(phase.metadata.args, { period_days: 30 });
  assert.equal(phase.metadata.is_error, false);
  assert.ok(Number.isFinite(phase.metadata.ms));
  assert.equal(missing.metadata.is_error, true);
  assert.equal(missing.errorCode, "mcp_input_error");
  assert.equal(missing.metadata.client, "ChatGPT");
  assert.match(missing.message, /キャベツ/);
  assert.equal(unknown.errorCode, "mcp_unknown_tool");
});

test("a failing logger never breaks the response", async () => {
  const data = createDataSource({ supabaseUrl: "https://example.supabase.co", anonKey: "anon", fetchImpl: mockFetch(makeRows(), []) });
  const handle = createMcpHandler({
    getRows: () => data.getRows(),
    logEvent: () => {
      throw new Error("db down");
    },
  });
  const res = await handle(new Request("http://x", { method: "POST", body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "list_items", arguments: {} } }) }));
  const body = await res.json();
  assert.equal(body.result.isError, undefined);
});

test("usage logger posts to usage_events with the fixed visitor id and clipped fields", async () => {
  const calls = [];
  const log = createUsageLogger({
    supabaseUrl: "https://example.supabase.co/",
    anonKey: "anon",
    fetchImpl: async (url, init) => {
      calls.push({ url, init });
      return new Response(null, { status: 201 });
    },
  });
  await log({ eventType: "mcp_call", pagePath: "/mcp/list_items", message: "x".repeat(500), metadata: { tool: "list_items" } });
  assert.equal(calls[0].url, "https://example.supabase.co/rest/v1/usage_events");
  const body = JSON.parse(calls[0].init.body);
  assert.equal(body.visitor_id, MCP_VISITOR_ID);
  assert.equal(body.event_type, "mcp_call");
  assert.equal(body.message_summary.length, 200);
  assert.equal(calls[0].init.headers.Prefer, "return=minimal");
  assert.equal(createUsageLogger({ supabaseUrl: "", anonKey: "" }), null);
  // 書き込みに失敗しても例外を投げない
  const failing = createUsageLogger({ supabaseUrl: "https://e", anonKey: "a", fetchImpl: async () => { throw new Error("offline"); } });
  await failing({ eventType: "mcp_call", pagePath: "/mcp/x" });
  assert.equal(clientLabel("python-httpx/0.27", "Cursor"), "Cursor");
  assert.equal(clientLabel("curl/8"), "その他");
});
