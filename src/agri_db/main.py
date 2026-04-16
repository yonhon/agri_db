import hashlib
import io
import os
import re
from datetime import date, datetime
from typing import Iterable
from urllib.parse import urljoin

import pdfplumber
import psycopg
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.oki-kyoudou.jp/Shikyo/shikyo.php"
PDF_LINK_PATTERN = re.compile(r"/Shikyo/PDF/HP.+?(\d{8})\.pdf", re.IGNORECASE)


def fetch_pdf_links(session: requests.Session) -> list[tuple[str, date]]:
    response = session.get(BASE_URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    links: list[tuple[str, date]] = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        match = PDF_LINK_PATTERN.search(href)
        if not match:
            continue
        yyyymmdd = match.group(1)
        sale_date = datetime.strptime(yyyymmdd, "%Y%m%d").date()
        absolute_url = urljoin(BASE_URL, href)
        links.append((absolute_url, sale_date))

    # URL重複を除外しつつ日付降順
    unique = {(u, d) for u, d in links}
    return sorted(unique, key=lambda x: x[1], reverse=True)


def extract_pdf_text(pdf_bytes: bytes) -> str:
    pages: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            pages.append(page.extract_text() or "")
    return "\n\n".join(pages).strip()


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
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
            """
        )
        cur.execute(
            """
            create index if not exists idx_source_files_sale_date
            on source_files(sale_date desc);
            """
        )
        cur.execute(
            """
            create or replace function touch_updated_at_source_files()
            returns trigger as $$
            begin
              new.updated_at = now();
              return new;
            end;
            $$ language plpgsql;
            """
        )
        cur.execute(
            """
            drop trigger if exists trg_source_files_updated_at on source_files;
            create trigger trg_source_files_updated_at
              before update on source_files
              for each row
              execute function touch_updated_at_source_files();
            """
        )
    conn.commit()


def upsert_source_file(
    conn: psycopg.Connection,
    sale_date: date,
    source_url: str,
    pdf_sha256: str,
    pdf_size_bytes: int,
    raw_text: str | None,
    parse_status: str,
    error_message: str | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into source_files (
              sale_date, source_url, pdf_sha256, pdf_size_bytes,
              raw_text, parse_status, error_message, fetched_at
            ) values (%s, %s, %s, %s, %s, %s, %s, now())
            on conflict (source_url) do update set
              sale_date = excluded.sale_date,
              pdf_sha256 = excluded.pdf_sha256,
              pdf_size_bytes = excluded.pdf_size_bytes,
              raw_text = excluded.raw_text,
              parse_status = excluded.parse_status,
              error_message = excluded.error_message,
              fetched_at = now();
            """,
            (
                sale_date,
                source_url,
                pdf_sha256,
                pdf_size_bytes,
                raw_text,
                parse_status,
                error_message,
            ),
        )
    conn.commit()


def process_links(conn: psycopg.Connection, session: requests.Session, links: Iterable[tuple[str, date]]) -> None:
    for source_url, sale_date in links:
        try:
            response = session.get(source_url, timeout=60)
            response.raise_for_status()
            pdf_bytes = response.content
            digest = sha256_hex(pdf_bytes)
            text = extract_pdf_text(pdf_bytes)
            upsert_source_file(
                conn=conn,
                sale_date=sale_date,
                source_url=source_url,
                pdf_sha256=digest,
                pdf_size_bytes=len(pdf_bytes),
                raw_text=text,
                parse_status="fetched",
                error_message=None,
            )
            print(f"[OK] {sale_date} {source_url}")
        except Exception as exc:  # noqa: BLE001
            upsert_source_file(
                conn=conn,
                sale_date=sale_date,
                source_url=source_url,
                pdf_sha256="",
                pdf_size_bytes=0,
                raw_text=None,
                parse_status="failed",
                error_message=str(exc)[:1000],
            )
            print(f"[NG] {sale_date} {source_url} {exc}")


def main() -> None:
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL is not set")

    with psycopg.connect(db_url) as conn:
        ensure_schema(conn)
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "agri-db-bot/1.0 (+https://github.com/)",
            }
        )
        links = fetch_pdf_links(session)
        if not links:
            raise RuntimeError("No PDF links found on source page")
        process_links(conn, session, links)


if __name__ == "__main__":
    main()

