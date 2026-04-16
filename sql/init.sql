create table if not exists source_files (
  id bigserial primary key,
  sale_date date not null,
  source_url text not null unique,
  pdf_sha256 text not null,
  pdf_size_bytes integer not null,
  raw_text text,
  parse_status text not null default 'fetched',
  error_message text,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
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

