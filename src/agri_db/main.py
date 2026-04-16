import hashlib
import html
import io
import os
import re
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable
from urllib.parse import urljoin

import pdfplumber
import psycopg
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.oki-kyoudou.jp/Shikyo/shikyo.php"
PDF_URL_TEMPLATE = "https://www.oki-kyoudou.jp/Shikyo/PDF/HP%E5%B8%82%E6%B3%81{yyyymmdd}.pdf"
DATE_PATTERN = re.compile(r"(20\d{6})")
PDF_URL_PATTERN = re.compile(r"""["']([^"'<>\\s]+\.pdf)["']""", re.IGNORECASE)
NUMBER_PATTERN = re.compile(r"\d[\d,]*")
NUMBER_TOKEN_PATTERN = re.compile(r"^\d[\d,]*$")


def fetch_pdf_links(session: requests.Session) -> list[tuple[str, date]]:
    response = session.get(BASE_URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    candidates: list[str] = []
    for tag in soup.find_all(["a", "area"], href=True):
        href = tag["href"].strip()
        if href:
            candidates.append(href)

    # フロント実装差分でaタグ以外にPDFリンクが埋まっている場合のフォールバック
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

    # URL重複を除外しつつ日付降順
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


def extract_pdf_text_basic(pdf_bytes: bytes) -> str:
    pages: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            pages.append(page.extract_text() or "")
    return "\n\n".join(pages).strip()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    def extract_page_text(page: Any) -> str:
        words = page.extract_words(
            x_tolerance=1.5,
            y_tolerance=2,
            use_text_flow=True,
            keep_blank_chars=False,
        )
        if not words:
            return page.extract_text() or ""

        # top座標ごとに行グルーピングし、x座標順に並べて行文字列を再構築
        line_map: dict[float, list[dict[str, Any]]] = {}
        for word in words:
            top_key = round(float(word.get("top", 0.0)), 1)
            line_map.setdefault(top_key, []).append(word)

        lines: list[str] = []
        for top_key in sorted(line_map.keys()):
            row_words = sorted(line_map[top_key], key=lambda w: float(w.get("x0", 0.0)))
            parts: list[str] = []
            prev_x1: float | None = None
            for word in row_words:
                x0 = float(word.get("x0", 0.0))
                x1 = float(word.get("x1", x0))
                text = str(word.get("text", ""))
                if prev_x1 is not None and (x0 - prev_x1) > 6.0:
                    parts.append(" ")
                parts.append(text)
                prev_x1 = x1
            line = "".join(parts).strip()
            if line:
                lines.append(line)
        return "\n".join(lines)

    pages: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            pages.append(extract_page_text(page))
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
            create or replace view source_files_jst as
            select
              id,
              sale_date,
              source_url,
              pdf_sha256,
              pdf_size_bytes,
              raw_text,
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
) -> int:
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
              fetched_at = now()
            returning id;
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
        row = cur.fetchone()
    conn.commit()
    if not row:
        raise RuntimeError("Failed to get source_files.id after upsert")
    return int(row[0])


def normalize_text_for_parse(text: str) -> str:
    normalized = text.translate(str.maketrans("０１２３４５６７８９，", "0123456789,"))
    return normalized.replace("\u3000", " ")


def parse_decimal(token: str) -> Decimal | None:
    try:
        return Decimal(token.replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def parse_market_rows(raw_text: str) -> list[dict]:
    rows: list[dict] = []
    for line_no, raw_line in enumerate(raw_text.splitlines(), start=1):
        line = normalize_text_for_parse(raw_line).strip()
        if not line:
            continue

        number_matches = list(NUMBER_PATTERN.finditer(line))
        if len(number_matches) < 4:
            continue

        parsed_numbers: list[Decimal] = []
        for m in number_matches:
            value = parse_decimal(m.group(0))
            if value is not None:
                parsed_numbers.append(value)
        if len(parsed_numbers) < 4:
            continue

        first_num_start = number_matches[0].start()
        item_name = line[:first_num_start].strip(" :")
        if not item_name:
            continue

        high_price, avg_price, low_price, quantity = parsed_numbers[-4:]
        rows.append(
            {
                "line_no": line_no,
                "raw_line": raw_line.strip(),
                "item_name": item_name,
                "quantity": quantity,
                "high_price": high_price,
                "avg_price": avg_price,
                "low_price": low_price,
                "parse_confidence": 60,
            }
        )
    return rows


def _is_numeric_token(text: str) -> bool:
    return bool(NUMBER_TOKEN_PATTERN.match(normalize_text_for_parse(text)))


def parse_market_rows_from_pdf(pdf_bytes: bytes) -> list[dict]:
    rows: list[dict] = []
    global_line_no = 0

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            words = page.extract_words(
                x_tolerance=1.5,
                y_tolerance=2.5,
                use_text_flow=True,
                keep_blank_chars=False,
            )
            if not words:
                continue

            # top座標を近い値でクラスタリングして、実質的な行を復元
            sorted_words = sorted(words, key=lambda w: (float(w.get("top", 0.0)), float(w.get("x0", 0.0))))
            lines: list[list[dict[str, Any]]] = []
            current: list[dict[str, Any]] = []
            current_top: float | None = None
            tolerance = 2.0

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

            for line_words in lines:
                global_line_no += 1
                line_words = sorted(line_words, key=lambda w: float(w.get("x0", 0.0)))
                tokens: list[dict[str, Any]] = []
                for word in line_words:
                    text = str(word.get("text", "")).strip()
                    if not text:
                        continue
                    tokens.append(
                        {
                            "text": text,
                            "norm": normalize_text_for_parse(text),
                            "x0": float(word.get("x0", 0.0)),
                        }
                    )
                if len(tokens) < 2:
                    continue

                numeric_indexes = [i for i, t in enumerate(tokens) if _is_numeric_token(t["norm"])]
                if len(numeric_indexes) < 4:
                    continue

                # 右側4列を価格/数量とみなす
                picked = numeric_indexes[-4:]
                parsed_values: list[Decimal] = []
                for idx in picked:
                    value = parse_decimal(tokens[idx]["norm"])
                    if value is None:
                        parsed_values = []
                        break
                    parsed_values.append(value)
                if len(parsed_values) != 4:
                    continue

                first_value_index = picked[0]
                name_parts = [t["text"] for t in tokens[:first_value_index] if t["text"]]
                item_name = "".join(name_parts).strip(" :")
                if not item_name:
                    continue

                raw_line = " ".join(t["text"] for t in tokens).strip()
                high_price, avg_price, low_price, quantity = parsed_values
                rows.append(
                    {
                        "line_no": global_line_no,
                        "raw_line": raw_line,
                        "item_name": item_name,
                        "quantity": quantity,
                        "high_price": high_price,
                        "avg_price": avg_price,
                        "low_price": low_price,
                        "parse_confidence": 80,
                    }
                )

    return rows


def replace_market_rows(conn: psycopg.Connection, source_file_id: int, rows: list[dict]) -> None:
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


def process_links(conn: psycopg.Connection, session: requests.Session, links: Iterable[tuple[str, date]]) -> None:
    for source_url, sale_date in links:
        try:
            response = session.get(source_url, timeout=60)
            response.raise_for_status()
            pdf_bytes = response.content
            digest = sha256_hex(pdf_bytes)
            text = extract_pdf_text(pdf_bytes)
            parsed_rows = parse_market_rows_from_pdf(pdf_bytes)
            if len(parsed_rows) < 10:
                # 座標抽出が崩れた場合はテキスト抽出ベースへフォールバック
                text_rows = parse_market_rows(text)
                fallback_text = extract_pdf_text_basic(pdf_bytes)
                fallback_rows = parse_market_rows(fallback_text)
                best_rows = parsed_rows
                if len(text_rows) > len(best_rows):
                    best_rows = text_rows
                if len(fallback_rows) > len(best_rows):
                    text = fallback_text
                    best_rows = fallback_rows
                parsed_rows = best_rows
            source_file_id = upsert_source_file(
                conn=conn,
                sale_date=sale_date,
                source_url=source_url,
                pdf_sha256=digest,
                pdf_size_bytes=len(pdf_bytes),
                raw_text=text,
                parse_status="fetched",
                error_message=None,
            )
            replace_market_rows(conn, source_file_id, parsed_rows)
            print(f"[OK] {sale_date} {source_url} rows={len(parsed_rows)}")
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
        print(f"[INFO] links from listing page: {len(links)}")
        if not links:
            links = probe_recent_pdf_links(session)
            print(f"[INFO] links from direct probe fallback: {len(links)}")
        if not links:
            raise RuntimeError("No PDF links found (listing + fallback probe)")
        process_links(conn, session, links)


if __name__ == "__main__":
    main()
