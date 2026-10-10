// Supabase Edge Function のエントリーポイント。
// 配備: supabase functions deploy agri-mcp --no-verify-jwt
//   （claude.ai のコネクタからはログインなしで呼ぶため、JWT 検証を無効にする。読み取り専用）
// SUPABASE_URL と SUPABASE_ANON_KEY は Edge Functions の実行環境に自動で設定される。
import { createDataSource } from "./data.js";
import { createMcpHandler } from "./mcp.js";

const data = createDataSource({
  supabaseUrl: Deno.env.get("SUPABASE_URL") ?? "",
  anonKey: Deno.env.get("SUPABASE_ANON_KEY") ?? "",
});

const handle = createMcpHandler({ getRows: () => data.getRows() });

Deno.serve(handle);
