create table if not exists source_files (
  id bigserial primary key,
  sale_date date not null,
  source_url text not null unique,
  pdf_sha256 text not null,
  pdf_size_bytes integer not null,
  raw_text text,
  caption_signature text,
  format_alert boolean not null default false,
  parse_status text not null default 'fetched',
  error_message text,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table source_files add column if not exists caption_signature text;
alter table source_files add column if not exists format_alert boolean not null default false;

create table if not exists ingest_metadata (
  key text primary key,
  value text not null,
  updated_at timestamptz not null default now()
);

create index if not exists idx_source_files_sale_date
on source_files(sale_date desc);

create or replace function touch_updated_at_source_files()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists trg_source_files_updated_at on source_files;
create trigger trg_source_files_updated_at
  before update on source_files
  for each row
  execute function touch_updated_at_source_files();

create table if not exists market_rows (
  id bigserial primary key,
  source_file_id bigint not null references source_files(id) on delete cascade,
  line_no integer not null,
  raw_line text not null,
  item_name text,
  quantity numeric,
  high_price numeric,
  avg_price numeric,
  low_price numeric,
  parse_confidence smallint not null default 0,
  created_at timestamptz not null default now(),
  unique(source_file_id, line_no)
);

create index if not exists idx_market_rows_source_file_id
on market_rows(source_file_id);

create index if not exists idx_market_rows_item_name
on market_rows(item_name);

drop view if exists source_files_jst;
create or replace view source_files_jst as
select
  id,
  sale_date,
  source_url,
  pdf_sha256,
  pdf_size_bytes,
  raw_text,
  caption_signature,
  format_alert,
  parse_status,
  error_message,
  fetched_at,
  created_at,
  updated_at,
  fetched_at at time zone 'Asia/Tokyo' as fetched_at_jst,
  created_at at time zone 'Asia/Tokyo' as created_at_jst,
  updated_at at time zone 'Asia/Tokyo' as updated_at_jst
from source_files;

drop view if exists market_rows_jst;
create or replace view market_rows_jst as
select
  id,
  source_file_id,
  line_no,
  raw_line,
  item_name,
  quantity,
  high_price,
  avg_price,
  low_price,
  parse_confidence,
  created_at,
  created_at at time zone 'Asia/Tokyo' as created_at_jst
from market_rows;

drop view if exists market_daily_item_stats;
create or replace view market_daily_item_stats as
select
  sf.sale_date,
  mr.item_name,
  sum(mr.quantity)::numeric as quantity,
  avg(mr.high_price)::numeric as high_price,
  avg(mr.avg_price)::numeric as avg_price,
  avg(mr.low_price)::numeric as low_price
from market_rows mr
join source_files sf on sf.id = mr.source_file_id
where sf.parse_status = 'fetched'
  and mr.item_name is not null
  and mr.item_name <> ''
group by sf.sale_date, mr.item_name;

create table if not exists usage_events (
  id bigserial primary key,
  event_at timestamptz not null default now(),
  visitor_id text not null,
  event_type text not null check (event_type in ('page_view', 'error')),
  page_path text not null,
  error_code text,
  message_summary text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_usage_events_event_at
on usage_events(event_at desc);

create index if not exists idx_usage_events_event_type
on usage_events(event_type, event_at desc);

create index if not exists idx_usage_events_visitor_id
on usage_events(visitor_id, event_at desc);

alter table usage_events enable row level security;

drop policy if exists usage_events_insert_anon on usage_events;
create policy usage_events_insert_anon
on usage_events
for insert
to anon, authenticated
with check (
  char_length(visitor_id) between 8 and 64
  and event_type in ('page_view', 'error')
  and char_length(page_path) between 1 and 255
  and (message_summary is null or char_length(message_summary) <= 200)
);

drop view if exists usage_daily_metrics_jst;
create or replace view usage_daily_metrics_jst as
select
  (event_at at time zone 'Asia/Tokyo')::date as day_jst,
  count(*) filter (where event_type = 'page_view')::bigint as pv,
  count(distinct visitor_id) filter (where event_type = 'page_view')::bigint as uu,
  count(*) filter (where event_type = 'error')::bigint as error_count
from usage_events
group by 1;

drop view if exists usage_monthly_metrics_jst;
create or replace view usage_monthly_metrics_jst as
select
  date_trunc('month', event_at at time zone 'Asia/Tokyo')::date as month_jst,
  count(*) filter (where event_type = 'page_view')::bigint as pv,
  count(distinct visitor_id) filter (where event_type = 'page_view')::bigint as uu,
  count(*) filter (where event_type = 'error')::bigint as error_count
from usage_events
group by 1;

drop view if exists usage_daily_user_pv_jst;
create or replace view usage_daily_user_pv_jst as
select
  (event_at at time zone 'Asia/Tokyo')::date as day_jst,
  visitor_id,
  count(*)::bigint as pv
from usage_events
where event_type = 'page_view'
group by 1, visitor_id;

drop view if exists usage_monthly_user_pv_jst;
create or replace view usage_monthly_user_pv_jst as
select
  date_trunc('month', event_at at time zone 'Asia/Tokyo')::date as month_jst,
  visitor_id,
  count(*)::bigint as pv
from usage_events
where event_type = 'page_view'
group by 1, visitor_id;

drop view if exists usage_error_latest_7d_jst;
create or replace view usage_error_latest_7d_jst as
select
  coalesce(error_code, 'unknown') as error_code,
  coalesce(nullif(message_summary, ''), '(no message)') as message_summary,
  count(*)::bigint as count_7d,
  max(event_at) as last_seen_at,
  max(event_at at time zone 'Asia/Tokyo') as last_seen_at_jst
from usage_events
where event_type = 'error'
  and event_at >= now() - interval '7 days'
group by 1, 2
order by count_7d desc, last_seen_at desc;

grant select on table usage_daily_metrics_jst to anon, authenticated;
grant select on table usage_monthly_metrics_jst to anon, authenticated;
grant select on table usage_daily_user_pv_jst to anon, authenticated;
grant select on table usage_monthly_user_pv_jst to anon, authenticated;
grant select on table usage_error_latest_7d_jst to anon, authenticated;
