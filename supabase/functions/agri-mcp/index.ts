// Supabase Edge Function のエントリーポイント。
// 配備: supabase functions deploy agri-mcp --no-verify-jwt
//   （claude.ai のコネクタからはログインなしで呼ぶため、JWT 検証を無効にする。読み取り専用）
// SUPABASE_URL と SUPABASE_ANON_KEY は Edge Functions の実行環境に自動で設定される。
import { createDataSource } from "./data.js";
import { createMcpHandler } from "./mcp.js";
import { createUsageLogger } from "./usage.js";

const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
const anonKey = Deno.env.get("SUPABASE_ANON_KEY") ?? "";

const data = createDataSource({ supabaseUrl, anonKey });
// 利用状況ログ（usage_events）への記録。応答を返した後も書き込みを続けられるよう EdgeRuntime.waitUntil を使う
const logEvent = createUsageLogger({ supabaseUrl, anonKey });
// deno-lint-ignore no-explicit-any
const edgeRuntime = (globalThis as any).EdgeRuntime;
const waitUntil = (p: Promise<unknown>) => (edgeRuntime?.waitUntil ? edgeRuntime.waitUntil(p) : p);

const handle = createMcpHandler({ getRows: () => data.getRows(), logEvent, waitUntil });

Deno.serve(handle);
