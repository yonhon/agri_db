-- 定期取り込み（.github/workflows/daily_ingest.yml）を Supabase の pg_cron から起動する。
-- GitHub Actions の定時起動（schedule）は数時間遅れることがあるため、GitHub の API で
-- 手動実行と同じ経路（workflow_dispatch）から起動する。
--
-- 事前に、GitHub のトークンを Vault に登録しておく（README の「6-1」を参照）。
-- トークンはこのファイルにも、リポジトリのどこにも書かない。
--
-- このファイルは何度実行してもよい（同じ名前のジョブは上書きされる）。

create extension if not exists pg_cron with schema pg_catalog;
create extension if not exists pg_net with schema extensions;

-- pg_cron の時刻は UTC。JST 12:17 = UTC 03:17、JST 14:47 = UTC 05:47
select cron.schedule(
  'agri-ingest-jst-1217',
  '17 3 * * *',
  $job$
  select net.http_post(
    url := 'https://api.github.com/repos/yonhon/agri_db/actions/workflows/daily_ingest.yml/dispatches',
    headers := jsonb_build_object(
      'Accept', 'application/vnd.github+json',
      'Authorization', 'Bearer ' || (
        select decrypted_secret from vault.decrypted_secrets where name = 'github_actions_dispatch_token'
      ),
      'X-GitHub-Api-Version', '2022-11-28',
      'Content-Type', 'application/json'
    ),
    body := '{"ref": "main"}'::jsonb,
    timeout_milliseconds := 10000
  );
  $job$
);

-- 予備実行（市況PDFの公開が遅れた日のため）
select cron.schedule(
  'agri-ingest-jst-1447',
  '47 5 * * *',
  $job$
  select net.http_post(
    url := 'https://api.github.com/repos/yonhon/agri_db/actions/workflows/daily_ingest.yml/dispatches',
    headers := jsonb_build_object(
      'Accept', 'application/vnd.github+json',
      'Authorization', 'Bearer ' || (
        select decrypted_secret from vault.decrypted_secrets where name = 'github_actions_dispatch_token'
      ),
      'X-GitHub-Api-Version', '2022-11-28',
      'Content-Type', 'application/json'
    ),
    body := '{"ref": "main"}'::jsonb,
    timeout_milliseconds := 10000
  );
  $job$
);

-- 確認用
--   登録されたジョブ:        select jobname, schedule, active from cron.job;
--   ジョブの実行結果:        select jobname, status, return_message, start_time
--                            from cron.job_run_details d join cron.job j using (jobid)
--                            order by start_time desc limit 5;
--   GitHub からの応答:       select status_code, content, error_msg, created
--                            from net._http_response order by created desc limit 5;
--                            （204 なら成功。401 はトークンの誤りか期限切れ、404 は権限不足）
-- 止めるとき
--   select cron.unschedule('agri-ingest-jst-1217');
--   select cron.unschedule('agri-ingest-jst-1447');
