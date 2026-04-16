import hashlib
import html
import io
import os
import re
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable
from urllib.parse import urljoin

import fitz  # PyMuPDF
import pdfplumber
import psycopg
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.oki-kyoudou.jp/Shikyo/shikyo.php"
PDF_URL_TEMPLATE = "https://www.oki-kyoudou.jp/Shikyo/PDF/HP%E5%B8%82%E6%B3%81{yyyymmdd}.pdf"
DATE_PATTERN = re.compile(r"(20\d{6})")
PDF_URL_PATTERN = re.compile(r"""["']([^"'<>\\s]+\.pdf)["']""", re.IGNORECASE)
ITEM_NAME_PATTERN = re.compile(r"[A-Za-z\u3040-\u30ff\u3400-\u9fff]")


def fetch_pdf_links(session: requests.Session) -> list[tuple[str, date]]:
    response = session.get(BASE_URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    candidates: list[str] = []
    for tag in soup.find_all(["a", "area"], href=True):
        href = tag["href"].strip()
        if href:
            candidates.append(href)
    candidates.extend(match.group(1) for match in PDF_URL_PATTERN.finditer(response.text))

    links: list[tuple[str, date]] = []
    for raw_url in candidates:
        normalized = html.unescape(raw_url).replace("\\", "/").strip()
        if ".pdf" not in normalized.lower():
            continue
        date_match = DATE_PATTERN.search(normalized)
        if not date_match:
            continue
        yyyymmdd = date_match.group(1)
        try:
            sale_date = datetime.strptime(yyyymmdd, "%Y%m%d").date()
        except ValueError:
            continue
        absolute_url = urljoin(BASE_URL, normalized)
        links.append((absolute_url, sale_date))

    unique = {(u, d) for u, d in links}
    return sorted(unique, key=lambda x: x[1], reverse=True)


def probe_recent_pdf_links(session: requests.Session, days_back: int = 45) -> list[tuple[str, date]]:
    results: list[tuple[str, date]] = []
    today = datetime.now().date()
    for offset in range(days_back + 1):
        target_date = today - timedelta(days=offset)
        yyyymmdd = target_date.strftime("%Y%m%d")
        url = PDF_URL_TEMPLATE.format(yyyymmdd=yyyymmdd)
        try:
            response = session.head(url, timeout=20, allow_redirects=True)
            if response.status_code == 405:
                response = session.get(url, timeout=30, stream=True)
            if response.status_code != 200:
                continue
            content_type = (response.headers.get("Content-Type") or "").lower()
            if "pdf" in content_type or url.lower().endswith(".pdf"):
                results.append((url, target_date))
        except requests.RequestException:
            continue
    return sorted(results, key=lambda x: x[1], reverse=True)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalize_text_for_parse(text: str) -> str:
    normalized = text.translate(str.maketrans("０１２３４５６７８９，．", "0123456789,."))
    return normalized.replace("\u3000", " ").strip()


def pick_item_label_only(text: str) -> str:
    compact = re.sub(r"\s+", " ", text.strip())
    if not compact:
        return ""
    return compact.split(" ")[0].strip()


def has_number(text: str) -> bool:
    return bool(re.search(r"\d", text))


def is_zero_like(text: str) -> bool:
    compact = re.sub(r"[\s,.\-]", "", text)
    return compact != "" and set(compact) == {"0"}


def pick_first_numeric_token(text: str) -> str:
    normalized = normalize_text_for_parse(text)
    match = re.search(r"\d[\d,]*(?:\.\d+)?", normalized)
    if match:
        return match.group(0)
    return ""


def parse_decimal_from_text(text: str) -> Decimal | None:
    token = pick_first_numeric_token(text)
    if not token:
        return None
    try:
        return Decimal(token.replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def normalize_row_values(row: list[str]) -> list[str]:
    if not row:
        return row
    if not has_number(" ".join(row[1:])):
        return row
    normalized = [row[0].strip()]
    for cell in row[1:]:
        token = pick_first_numeric_token(cell)
        normalized.append(token if token else cell.strip())
    return normalized


def get_pymupdf_page_words(pdf_bytes: bytes, page_idx: int) -> list[dict[str, Any]]:
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        if page_idx < 0 or page_idx >= len(doc):
            return []
        page = doc[page_idx]
        words = page.get_text("words")
    out: list[dict[str, Any]] = []
    for w in words:
        out.append(
            {
                "x0": float(w[0]),
                "top": float(w[1]),
                "x1": float(w[2]),
                "bottom": float(w[3]),
                "text": str(w[4]).strip(),
            }
        )
    return out


def cluster_words_to_lines(words: list[dict[str, Any]], tolerance: float = 2.0) -> list[str]:
    sorted_words = sorted(words, key=lambda w: (float(w.get("top", 0.0)), float(w.get("x0", 0.0))))
    lines: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_top: float | None = None
    for word in sorted_words:
        top = float(word.get("top", 0.0))
        if current_top is None or abs(top - current_top) <= tolerance:
            current.append(word)
            current_top = top if current_top is None else (current_top + top) / 2
        else:
            lines.append(current)
            current = [word]
            current_top = top
    if current:
        lines.append(current)

    out: list[str] = []
    for line_words in lines:
        parts = [str(w.get("text", "")).strip() for w in sorted(line_words, key=lambda w: float(w.get("x0", 0.0)))]
        text = "".join(p for p in parts if p).strip()
        if text and not is_zero_like(text):
            out.append(text)
    return out


def _row_bbox_from_pdfplumber_row(row: Any) -> tuple[float, float, float, float] | None:
    bbox = getattr(row, "bbox", None)
    if bbox and len(bbox) == 4:
        return (float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3]))
    cells = getattr(row, "cells", None)
    if not cells:
        return None
    valid = [c for c in cells if c is not None and len(c) == 4]
    if not valid:
        return None
    return (
        min(float(c[0]) for c in valid),
        min(float(c[1]) for c in valid),
        max(float(c[2]) for c in valid),
        max(float(c[3]) for c in valid),
    )


def _column_xranges_from_table(table: Any, ncols: int) -> list[tuple[float, float] | None]:
    rows = getattr(table, "rows", None) or []
    ranges: list[list[tuple[float, float]]] = [[] for _ in range(ncols)]
    for row in rows:
        cells = getattr(row, "cells", None) or []
        for cidx in range(min(ncols, len(cells))):
            cell = cells[cidx]
            if cell is None or len(cell) != 4:
                continue
            ranges[cidx].append((float(cell[0]), float(cell[2])))
    out: list[tuple[float, float] | None] = []
    for col_ranges in ranges:
        if not col_ranges:
            out.append(None)
        else:
            out.append((min(a for a, _ in col_ranges), max(b for _, b in col_ranges)))
    return out


def _join_words_in_bbox(words: list[dict[str, Any]], bbox: tuple[float, float, float, float]) -> str:
    x0, y0, x1, y1 = bbox
    picked: list[dict[str, Any]] = []
    for w in words:
        if not w["text"]:
            continue
        if w["x1"] < x0 or w["x0"] > x1:
            continue
        if w["bottom"] < y0 or w["top"] > y1:
            continue
        picked.append(w)
    if not picked:
        return ""
    picked = sorted(picked, key=lambda w: (w["top"], w["x0"]))
    return " ".join(w["text"] for w in picked).strip()


def fill_missing_cells_from_pymupdf(table: Any, data: list[list[Any]], page_words: list[dict[str, Any]]) -> list[list[str]]:
    rows = [[str(c).replace("\n", " ").strip() if c is not None else "" for c in row] for row in data]
    if not rows:
        return rows
    ncols = max(len(r) for r in rows)
    col_ranges = _column_xranges_from_table(table, ncols)
    table_rows = getattr(table, "rows", None) or []
    for ridx, row in enumerate(rows):
        if ridx >= len(table_rows):
            continue
        row_bbox = _row_bbox_from_pdfplumber_row(table_rows[ridx])
        if row_bbox is None:
            continue
        _, ry0, _, ry1 = row_bbox
        for cidx in range(len(row)):
            if row[cidx]:
                continue
            col = col_ranges[cidx] if cidx < len(col_ranges) else None
            if col is None:
                continue
            cx0, cx1 = col
            row[cidx] = _join_words_in_bbox(page_words, (cx0 - 0.8, ry0 - 1.5, cx1 + 0.8, ry1 + 1.5))
    return rows


def _first_col_xrange_from_table(table: Any) -> tuple[float, float] | None:
    rows = getattr(table, "rows", None) or []
    xs: list[tuple[float, float]] = []
    for row in rows:
        cells = getattr(row, "cells", None) or []
        if not cells:
            continue
        c0 = cells[0]
        if c0 is not None and len(c0) == 4:
            xs.append((float(c0[0]), float(c0[2])))
    if not xs:
        return None
    return (min(a for a, _ in xs), max(b for _, b in xs))


def extract_first_col_by_row_pymupdf(page_words: list[dict[str, Any]], table: Any) -> list[str]:
    x0, top, x1, bottom = table.bbox
    x_range = _first_col_xrange_from_table(table)
    if x_range is None:
        x_range = (x0, x0 + (x1 - x0) * 0.35)
    col_x0, col_x1 = x_range

    left_words: list[dict[str, Any]] = []
    for w in page_words:
        if w["x1"] < col_x0 or w["x0"] > col_x1:
            continue
        if w["bottom"] < top or w["top"] > bottom:
            continue
        left_words.append(w)

    rows = getattr(table, "rows", None) or []
    out: list[str] = []
    for row in rows:
        rb = _row_bbox_from_pdfplumber_row(row)
        if rb is None:
            out.append("")
            continue
        _, ry0, _, ry1 = rb
        row_words = [w for w in left_words if not (w["bottom"] < ry0 - 1.2 or w["top"] > ry1 + 1.2)]
        lines = cluster_words_to_lines(row_words, tolerance=1.8)
        picked = ""
        for line in lines:
            s = line.strip()
            if not s or is_zero_like(s) or has_number(s):
                continue
            picked = pick_item_label_only(s)
            break
        out.append(picked)
    return out


def fill_first_col_by_row_values(data: list[list[str]], first_col_values: list[str]) -> list[list[str]]:
    rows = [row[:] for row in data]
    for ridx, row in enumerate(rows):
        if ridx >= len(first_col_values) or not row:
            continue
        candidate = pick_item_label_only(first_col_values[ridx])
        if candidate:
            row[0] = candidate
    return rows


def extract_raw_text_basic(pdf_bytes: bytes) -> str:
    pages: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for index, page in enumerate(pdf.pages, start=1):
            pages.append(f"=== PAGE {index} ===\n{page.extract_text() or ''}")
    return "\n\n".join(pages).strip()


def canonicalize_caption(text: str) -> str:
    t = normalize_text_for_parse(text)
    t = re.sub(r"[\s/／()（）\[\]【】]+", "", t)
    return t


def extract_market_rows_from_pdf(pdf_bytes: bytes) -> tuple[list[dict[str, Any]], str]:
    rows_out: list[dict[str, Any]] = []
    line_no = 0
    caption_signature = ""

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            tables = page.find_tables()
            if not tables:
                continue
            page_words = get_pymupdf_page_words(pdf_bytes, page_idx)
            for table in tables:
                extracted = table.extract() or []
                if not extracted:
                    continue
                merged = fill_missing_cells_from_pymupdf(table, extracted, page_words)
                first_col = extract_first_col_by_row_pymupdf(page_words, table)
                merged = fill_first_col_by_row_values(merged, first_col)

                for ridx, row in enumerate(merged):
                    normalized_row = normalize_row_values(row)
                    if ridx == 0 and not caption_signature:
                        caption_signature = "|".join(canonicalize_caption(c) for c in normalized_row[:5] if c.strip())

                    if len(normalized_row) < 5:
                        continue

                    item_name = pick_item_label_only(normalized_row[0])
                    if not item_name or not ITEM_NAME_PATTERN.search(item_name):
                        continue

                    quantity = parse_decimal_from_text(normalized_row[1])
                    high_price = parse_decimal_from_text(normalized_row[2])
                    avg_price = parse_decimal_from_text(normalized_row[3])
                    low_price = parse_decimal_from_text(normalized_row[4])
                    if any(v is None for v in (quantity, high_price, avg_price, low_price)):
                        continue
                    if high_price == 0 and avg_price == 0 and low_price == 0 and quantity == 0:
                        continue

                    line_no += 1
                    rows_out.append(
                        {
                            "line_no": line_no,
                            "raw_line": "\t".join(normalized_row[:5]),
                            "item_name": item_name,
                            "quantity": quantity,
                            "high_price": high_price,
                            "avg_price": avg_price,
                            "low_price": low_price,
                            "parse_confidence": 95,
                        }
                    )

    return rows_out, caption_signature


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
              caption_signature text,
              format_alert boolean not null default false,
              parse_status text not null default 'fetched',
              error_message text,
              fetched_at timestamptz not null default now(),
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );
            """
        )
        cur.execute("alter table source_files add column if not exists caption_signature text;")
        cur.execute("alter table source_files add column if not exists format_alert boolean not null default false;")
        cur.execute(
            """
            create table if not exists ingest_metadata (
              key text primary key,
              value text not null,
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
        cur.execute(
            """
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
            """
        )
        cur.execute(
            """
            create index if not exists idx_market_rows_source_file_id
            on market_rows(source_file_id);
            """
        )
        cur.execute(
            """
            create index if not exists idx_market_rows_item_name
            on market_rows(item_name);
            """
        )
        cur.execute(
            """
            drop view if exists source_files_jst;
            """
        )
        cur.execute(
            """
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
            """
        )
        cur.execute(
            """
            drop view if exists market_rows_jst;
            """
        )
        cur.execute(
            """
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
            """
        )
        cur.execute(
            """
            drop view if exists market_daily_item_stats;
            """
        )
        cur.execute(
            """
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
            """
        )
        # Ensure PostgREST refreshes schema cache after view/index updates.
        cur.execute("select pg_notify('pgrst', 'reload schema');")
    conn.commit()


def detect_caption_change(conn: psycopg.Connection, current_signature: str) -> tuple[bool, str | None]:
    if not current_signature:
        return False, None
    previous: str | None = None
    with conn.cursor() as cur:
        cur.execute("select value from ingest_metadata where key = 'table_caption_last_seen';")
        row = cur.fetchone()
        if row:
            previous = str(row[0])
        cur.execute(
            """
            insert into ingest_metadata (key, value, updated_at)
            values ('table_caption_last_seen', %s, now())
            on conflict (key) do update set value = excluded.value, updated_at = now();
            """,
            (current_signature,),
        )
    conn.commit()
    return bool(previous and previous != current_signature), previous


def upsert_source_file(
    conn: psycopg.Connection,
    sale_date: date,
    source_url: str,
    pdf_sha256: str,
    pdf_size_bytes: int,
    raw_text: str | None,
    caption_signature: str | None,
    format_alert: bool,
    parse_status: str,
    error_message: str | None,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into source_files (
              sale_date, source_url, pdf_sha256, pdf_size_bytes, raw_text,
              caption_signature, format_alert, parse_status, error_message, fetched_at
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (source_url) do update set
              sale_date = excluded.sale_date,
              pdf_sha256 = excluded.pdf_sha256,
              pdf_size_bytes = excluded.pdf_size_bytes,
              raw_text = excluded.raw_text,
              caption_signature = excluded.caption_signature,
              format_alert = excluded.format_alert,
              parse_status = excluded.parse_status,
              error_message = excluded.error_message,
              fetched_at = now()
            returning id;
            """,
            (
                sale_date,
                source_url,
                pdf_sha256,
                pdf_size_bytes,
                raw_text,
                caption_signature,
                format_alert,
                parse_status,
                error_message,
            ),
        )
        row = cur.fetchone()
    conn.commit()
    if not row:
        raise RuntimeError("Failed to get source_files.id after upsert")
    return int(row[0])


def get_source_file_snapshot(conn: psycopg.Connection, source_url: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select id, pdf_sha256
            from source_files
            where source_url = %s;
            """,
            (source_url,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {"id": int(row[0]), "pdf_sha256": str(row[1] or "")}


def touch_source_file_fetched(
    conn: psycopg.Connection,
    source_file_id: int,
    sale_date: date,
    pdf_sha256: str,
    pdf_size_bytes: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update source_files
            set sale_date = %s,
                pdf_sha256 = %s,
                pdf_size_bytes = %s,
                parse_status = 'fetched',
                error_message = null,
                fetched_at = now()
            where id = %s;
            """,
            (sale_date, pdf_sha256, pdf_size_bytes, source_file_id),
        )
    conn.commit()


def replace_market_rows(conn: psycopg.Connection, source_file_id: int, rows: list[dict[str, Any]]) -> None:
    with conn.cursor() as cur:
        cur.execute("delete from market_rows where source_file_id = %s;", (source_file_id,))
        for row in rows:
            cur.execute(
                """
                insert into market_rows (
                  source_file_id, line_no, raw_line, item_name,
                  quantity, high_price, avg_price, low_price, parse_confidence
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    source_file_id,
                    row["line_no"],
                    row["raw_line"],
                    row["item_name"],
                    row["quantity"],
                    row["high_price"],
                    row["avg_price"],
                    row["low_price"],
                    row["parse_confidence"],
                ),
            )
    conn.commit()


def process_links(conn: psycopg.Connection, session: requests.Session, links: Iterable[tuple[str, date]]) -> list[dict[str, str]]:
    alerts: list[dict[str, str]] = []
    for source_url, sale_date in links:
        try:
            response = session.get(source_url, timeout=60)
            response.raise_for_status()
            pdf_bytes = response.content
            digest = sha256_hex(pdf_bytes)
            existing = get_source_file_snapshot(conn, source_url)
            if existing and existing["pdf_sha256"] == digest:
                touch_source_file_fetched(
                    conn=conn,
                    source_file_id=int(existing["id"]),
                    sale_date=sale_date,
                    pdf_sha256=digest,
                    pdf_size_bytes=len(pdf_bytes),
                )
                print(f"[SKIP] {sale_date} {source_url} unchanged_sha256={digest}")
                continue

            raw_text = extract_raw_text_basic(pdf_bytes)
            parsed_rows, caption_signature = extract_market_rows_from_pdf(pdf_bytes)
            format_alert, previous_signature = detect_caption_change(conn, caption_signature)

            source_file_id = upsert_source_file(
                conn=conn,
                sale_date=sale_date,
                source_url=source_url,
                pdf_sha256=digest,
                pdf_size_bytes=len(pdf_bytes),
                raw_text=raw_text,
                caption_signature=caption_signature,
                format_alert=format_alert,
                parse_status="fetched",
                error_message=None,
            )
            replace_market_rows(conn, source_file_id, parsed_rows)

            if format_alert:
                alerts.append(
                    {
                        "sale_date": str(sale_date),
                        "source_url": source_url,
                        "previous_signature": previous_signature or "",
                        "current_signature": caption_signature,
                    }
                )
                print(
                    "::warning title=CaptionChanged::"
                    f"{sale_date} caption changed. prev={previous_signature} current={caption_signature}"
                )
            print(f"[OK] {sale_date} {source_url} rows={len(parsed_rows)}")
        except Exception as exc:  # noqa: BLE001
            upsert_source_file(
                conn=conn,
                sale_date=sale_date,
                source_url=source_url,
                pdf_sha256="",
                pdf_size_bytes=0,
                raw_text=None,
                caption_signature=None,
                format_alert=False,
                parse_status="failed",
                error_message=str(exc)[:1000],
            )
            print(f"[NG] {sale_date} {source_url} {exc}")
    return alerts


def raise_on_format_alerts(alerts: list[dict[str, str]]) -> None:
    if not alerts:
        return
    for alert in alerts:
        print(
            "::error title=FormatAlert::"
            f"{alert['sale_date']} {alert['source_url']} "
            f"prev={alert['previous_signature']} current={alert['current_signature']}"
        )
    raise RuntimeError(f"format_alert detected in {len(alerts)} file(s)")


def main() -> None:
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL is not set")

    with psycopg.connect(db_url) as conn:
        ensure_schema(conn)
        session = requests.Session()
        session.headers.update({"User-Agent": "agri-db-bot/1.0 (+https://github.com/)"})
        links = fetch_pdf_links(session)
        print(f"[INFO] links from listing page: {len(links)}")
        if not links:
            links = probe_recent_pdf_links(session)
            print(f"[INFO] links from direct probe fallback: {len(links)}")
        if not links:
            raise RuntimeError("No PDF links found (listing + fallback probe)")
        alerts = process_links(conn, session, links)
        raise_on_format_alerts(alerts)


if __name__ == "__main__":
    main()
