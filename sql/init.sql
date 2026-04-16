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
