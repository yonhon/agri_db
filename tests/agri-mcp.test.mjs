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
