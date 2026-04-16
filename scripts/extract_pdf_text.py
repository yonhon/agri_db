import argparse
import io
import re
import tempfile
from pathlib import Path
from typing import Any

import pdfplumber
import requests


def extract_basic(page: Any) -> str:
    return page.extract_text() or ""


def extract_layout(page: Any) -> str:
    return page.extract_text(layout=True) or ""


def extract_words(page: Any) -> str:
    words = page.extract_words(
        x_tolerance=1.5,
        y_tolerance=2.5,
        use_text_flow=True,
        keep_blank_chars=False,
    )
    if not words:
        return ""

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

    out_lines: list[str] = []
    for line_words in lines:
        row_words = sorted(line_words, key=lambda w: float(w.get("x0", 0.0)))
        parts: list[str] = []
        prev_x1: float | None = None
        for word in row_words:
            text = str(word.get("text", "")).strip()
            if not text:
                continue
            x0 = float(word.get("x0", 0.0))
            x1 = float(word.get("x1", x0))
            if prev_x1 is not None and (x0 - prev_x1) > 6.0:
                parts.append(" ")
            parts.append(text)
            prev_x1 = x1
        line = "".join(parts).strip()
        if line:
            out_lines.append(line)
    return "\n".join(out_lines)


def read_pdf_bytes(url: str | None, pdf_path: str | None) -> bytes:
    if url:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        return response.content
    if pdf_path:
        return Path(pdf_path).read_bytes()
    raise ValueError("Either --url or --pdf-path is required")


def extract_pdf_text(pdf_bytes: bytes, method: str) -> str:
    pages: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for index, page in enumerate(pdf.pages, start=1):
            if method == "basic":
                text = extract_basic(page)
            elif method == "layout":
                text = extract_layout(page)
            elif method == "words":
                text = extract_words(page)
            elif method == "tables":
                text = ""
            else:
                raise ValueError(f"Unsupported method: {method}")
            pages.append(f"=== PAGE {index} ===\n{text}")
    return "\n\n".join(pages).strip()


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def cell_to_text(cell: Any) -> str:
    if cell is None:
        return ""
    return str(cell).replace("\n", " ").strip()


def is_zero_like(text: str) -> bool:
    compact = re.sub(r"[\s,.\-]", "", text)
    return compact != "" and set(compact) == {"0"}


def has_number(text: str) -> bool:
    return bool(re.search(r"\d", text))


def pick_item_label_only(text: str) -> str:
    compact = re.sub(r"\s+", " ", text.strip())
    if not compact:
        return ""
    # "ほうれん草 鹿児島県 沖縄県" のような結合文字列は先頭語のみ採用
    parts = compact.split(" ")
    return parts[0].strip()


def pick_first_numeric_token(text: str) -> str:
    match = re.search(r"\d[\d,]*(?:\.\d+)?", text)
    if match:
        return match.group(0)
    return ""


def normalize_row_values(row: list[str]) -> list[str]:
    if not row:
        return row

    first_col = row[0].strip()
    # ヘッダ行や説明行はそのまま返す
    if not has_number(" ".join(row[1:])):
        return row

    normalized: list[str] = [first_col]
    for cell in row[1:]:
        token = pick_first_numeric_token(cell)
        normalized.append(token if token else cell.strip())
    return normalized


def extract_left_candidates(page: Any, table_bbox: tuple[float, float, float, float]) -> list[str]:
    x0, top, x1, bottom = table_bbox
    left_limit = x0 + (x1 - x0) * 0.35
    words = page.extract_words(
        x_tolerance=1.5,
        y_tolerance=2.5,
        use_text_flow=True,
        keep_blank_chars=False,
    )
    filtered = []
    for w in words:
        wx0 = float(w.get("x0", 0.0))
        wtop = float(w.get("top", 0.0))
        if x0 <= wx0 <= left_limit and top <= wtop <= bottom:
            filtered.append(w)

    sorted_words = sorted(filtered, key=lambda w: (float(w.get("top", 0.0)), float(w.get("x0", 0.0))))
    lines: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_top: float | None = None
    tolerance = 2.0
    for word in sorted_words:
        wtop = float(word.get("top", 0.0))
        if current_top is None or abs(wtop - current_top) <= tolerance:
            current.append(word)
            current_top = wtop if current_top is None else (current_top + wtop) / 2
        else:
            lines.append(current)
            current = [word]
            current_top = wtop
    if current:
        lines.append(current)

    out: list[str] = []
    for line_words in lines:
        parts = [str(w.get("text", "")).strip() for w in sorted(line_words, key=lambda w: float(w.get("x0", 0.0)))]
        text = "".join(p for p in parts if p).strip()
        if text and not is_zero_like(text):
            out.append(text)
    return out


def cluster_words_to_lines(words: list[dict[str, Any]], tolerance: float = 2.0) -> list[str]:
    sorted_words = sorted(words, key=lambda w: (float(w.get("top", 0.0)), float(w.get("x0", 0.0))))
    lines: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_top: float | None = None
    for word in sorted_words:
        wtop = float(word.get("top", 0.0))
        if current_top is None or abs(wtop - current_top) <= tolerance:
            current.append(word)
            current_top = wtop if current_top is None else (current_top + wtop) / 2
        else:
            lines.append(current)
            current = [word]
            current_top = wtop
    if current:
        lines.append(current)

    out: list[str] = []
    for line_words in lines:
        parts = [str(w.get("text", "")).strip() for w in sorted(line_words, key=lambda w: float(w.get("x0", 0.0)))]
        text = "".join(p for p in parts if p).strip()
        if text and not is_zero_like(text):
            out.append(text)
    return out


def extract_left_candidates_pymupdf(
    pdf_bytes: bytes,
    page_idx: int,
    table_bbox: tuple[float, float, float, float],
) -> list[str]:
    try:
        import fitz  # PyMuPDF
    except Exception:
        return []

    x0, top, x1, bottom = table_bbox
    left_limit = x0 + (x1 - x0) * 0.35
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        if page_idx < 0 or page_idx >= len(doc):
            return []
        page = doc[page_idx]
        words = page.get_text("words")  # x0,y0,x1,y1,text,...
        normalized_words: list[dict[str, Any]] = []
        for w in words:
            wx0, wy0, wx1, wy1, text = w[0], w[1], w[2], w[3], str(w[4])
            if x0 <= wx0 <= left_limit and top <= wy0 <= bottom:
                normalized_words.append({"x0": wx0, "top": wy0, "x1": wx1, "bottom": wy1, "text": text})
    return cluster_words_to_lines(normalized_words, tolerance=2.0)


def get_pymupdf_page_words(pdf_bytes: bytes, page_idx: int) -> list[dict[str, Any]]:
    try:
        import fitz  # PyMuPDF
    except Exception:
        return []
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
    x0 = min(float(c[0]) for c in valid)
    y0 = min(float(c[1]) for c in valid)
    x1 = max(float(c[2]) for c in valid)
    y1 = max(float(c[3]) for c in valid)
    return (x0, y0, x1, y1)


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
    rows = [[cell_to_text(c) for c in row] for row in data]
    if not rows or not page_words:
        return rows
    ncols = max(len(r) for r in rows)
    col_ranges = _column_xranges_from_table(table, ncols)
    table_rows = getattr(table, "rows", None) or []
    y_margin = 1.5
    x_margin = 0.8
    for ridx, row in enumerate(rows):
        if ridx >= len(table_rows):
            continue
        row_bbox = _row_bbox_from_pdfplumber_row(table_rows[ridx])
        if row_bbox is None:
            continue
        _, ry0, _, ry1 = row_bbox
        for cidx in range(len(row)):
            if row[cidx].strip():
                continue
            xr = col_ranges[cidx] if cidx < len(col_ranges) else None
            if xr is None:
                continue
            cx0, cx1 = xr
            text = _join_words_in_bbox(
                page_words,
                (cx0 - x_margin, ry0 - y_margin, cx1 + x_margin, ry1 + y_margin),
            )
            if text:
                row[cidx] = text
    return rows


def extract_first_col_by_row_pymupdf(pdf_bytes: bytes, page_idx: int, table: Any) -> list[str]:
    page_words = get_pymupdf_page_words(pdf_bytes, page_idx)
    if not page_words:
        return []
    table_bbox = table.bbox
    x0, top, x1, bottom = table_bbox
    x_range = _first_col_xrange_from_table(table)
    if x_range is None:
        x_range = (x0, x0 + (x1 - x0) * 0.35)
    col_x0, col_x1 = x_range

    normalized_words: list[dict[str, Any]] = []
    for w in page_words:
        wx0, wy0, wx1, wy1, text = w["x0"], w["top"], w["x1"], w["bottom"], str(w["text"]).strip()
        if not text:
            continue
        if wx1 < col_x0 or wx0 > col_x1:
            continue
        if wy1 < top or wy0 > bottom:
            continue
        normalized_words.append({"x0": wx0, "top": wy0, "x1": wx1, "bottom": wy1, "text": text})

    rows = getattr(table, "rows", None) or []
    out: list[str] = []
    y_margin = 1.2
    for row in rows:
        rb = _row_bbox_from_pdfplumber_row(row)
        if rb is None:
            out.append("")
            continue
        _, ry0, _, ry1 = rb
        row_words = []
        for w in normalized_words:
            if w["bottom"] < (ry0 - y_margin) or w["top"] > (ry1 + y_margin):
                continue
            row_words.append(w)
        lines = cluster_words_to_lines(row_words, tolerance=1.8)
        # 行内で最上段の語を優先。数字だけ/0だけの行は除外。
        picked = ""
        for line in lines:
            s = line.strip()
            if not s:
                continue
            if is_zero_like(s):
                continue
            if has_number(s):
                continue
            picked = pick_item_label_only(s)
            break
        out.append(picked)
    return out


def extract_left_candidates_unstructured(pdf_bytes: bytes, page_idx: int) -> list[str]:
    try:
        from unstructured.partition.pdf import partition_pdf
    except Exception:
        return []

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
        tmp.write(pdf_bytes)
        tmp.flush()
        try:
            elements = partition_pdf(
                filename=tmp.name,
                strategy="fast",
                include_metadata=True,
            )
        except Exception:
            return []

    candidates: list[str] = []
    page_no = page_idx + 1
    for e in elements:
        meta = getattr(e, "metadata", None)
        if not meta or getattr(meta, "page_number", None) != page_no:
            continue
        text = str(getattr(e, "text", "")).strip()
        if not text:
            continue
        # 1列目候補として日本語を含む短め文字列を優先
        if not re.search(r"[\u3040-\u30ff\u3400-\u9fff]", text):
            continue
        if len(text) > 30:
            continue
        if has_number(text):
            continue
        if text in candidates:
            continue
        candidates.append(text)
    return candidates


def looks_missing_first_col(first_cell: str, row_values: list[str]) -> bool:
    if first_cell == "" or is_zero_like(first_cell):
        return True
    # 1列目に数値しか無い場合は欠損の可能性が高い（品目名は日本語想定）
    if has_number(first_cell) and not re.search(r"[A-Za-z\u3040-\u30ff\u3400-\u9fff]", first_cell):
        return True
    return False


def fill_first_col(data: list[list[Any]], candidates: list[str]) -> list[list[str]]:
    rows = [[cell_to_text(c) for c in row] for row in data]
    if not rows or not candidates:
        return rows

    used: set[int] = set()
    row_count = len(rows)
    cand_count = len(candidates)
    for ridx, row in enumerate(rows):
        first = row[0] if row else ""
        rest = row[1:] if len(row) > 1 else []
        row_has_numbers = any(has_number(v) for v in rest)
        if not row_has_numbers:
            continue
        if not looks_missing_first_col(first, row):
            continue

        base = int((ridx + 0.5) * cand_count / max(row_count, 1))
        base = max(0, min(cand_count - 1, base))
        pick: int | None = None
        for dist in range(cand_count):
            for idx in (base - dist, base + dist):
                if 0 <= idx < cand_count and idx not in used:
                    pick = idx
                    break
            if pick is not None:
                break
        if pick is None:
            continue
        row[0] = candidates[pick]
        used.add(pick)
    return rows


def fill_first_col_by_row_values(
    data: list[list[Any]],
    first_col_values: list[str],
    force_replace: bool = False,
) -> list[list[str]]:
    rows = [[cell_to_text(c) for c in row] for row in data]
    for ridx, row in enumerate(rows):
        if ridx >= len(first_col_values):
            break
        candidate = pick_item_label_only(first_col_values[ridx].strip())
        if not candidate:
            continue
        if len(row) == 0:
            continue
        first = row[0]
        if force_replace:
            row[0] = candidate
            continue
        if looks_missing_first_col(first, row):
            row[0] = candidate
    return rows


def extract_tables(pdf_bytes: bytes, page_index_1based: int | None = None) -> str:
    lines: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page_indexes = (
            [page_index_1based - 1]
            if page_index_1based is not None
            else list(range(len(pdf.pages)))
        )
        for page_idx in page_indexes:
            if page_idx < 0 or page_idx >= len(pdf.pages):
                continue
            page = pdf.pages[page_idx]
            tables = page.find_tables()
            lines.append(f"=== PAGE {page_idx + 1} / tables={len(tables)} ===")
            if not tables:
                continue
            for table_idx, table in enumerate(tables, start=1):
                lines.append(f"--- TABLE {table_idx} ---")
                data = table.extract() or []
                for row in data:
                    row_values = [cell_to_text(c) for c in row]
                    lines.append("\t".join(row_values))
                lines.append("")
    return "\n".join(lines).strip()


def extract_tables_with_first_col_restore(
    pdf_bytes: bytes,
    page_index_1based: int | None = None,
    left_source: str = "pdfplumber",
) -> str:
    lines: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page_indexes = (
            [page_index_1based - 1]
            if page_index_1based is not None
            else list(range(len(pdf.pages)))
        )
        for page_idx in page_indexes:
            if page_idx < 0 or page_idx >= len(pdf.pages):
                continue
            page = pdf.pages[page_idx]
            tables = page.find_tables()
            page_words = get_pymupdf_page_words(pdf_bytes, page_idx) if left_source == "pymupdf" else []
            lines.append(f"=== PAGE {page_idx + 1} / tables={len(tables)} ===")
            if not tables:
                continue
            for table_idx, table in enumerate(tables, start=1):
                data = table.extract() or []
                if left_source == "pdfplumber":
                    candidates = extract_left_candidates(page, table.bbox)
                    merged = fill_first_col(data, candidates)
                elif left_source == "pymupdf":
                    candidates = extract_left_candidates_pymupdf(pdf_bytes, page_idx, table.bbox)
                    merged_with_cells = fill_missing_cells_from_pymupdf(table, data, page_words)
                    by_row = extract_first_col_by_row_pymupdf(pdf_bytes, page_idx, table)
                    merged = fill_first_col_by_row_values(merged_with_cells, by_row, force_replace=True)
                elif left_source == "unstructured":
                    candidates = extract_left_candidates_unstructured(pdf_bytes, page_idx)
                    merged = fill_first_col(data, candidates)
                else:
                    candidates = []
                    merged = [[cell_to_text(c) for c in row] for row in data]
                lines.append(f"--- TABLE {table_idx} / left_source={left_source} / left_candidates={len(candidates)} ---")
                for row in merged:
                    normalized_row = normalize_row_values(row)
                    lines.append("\t".join(normalized_row))
                lines.append("")
    return "\n".join(lines).strip()


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract text from PDF for parser tuning.")
    parser.add_argument("--url", help="PDF URL", default=None)
    parser.add_argument("--pdf-path", help="Local PDF path", default=None)
    parser.add_argument("--method", choices=["basic", "layout", "words", "tables", "tables_firstcol", "all"], default="all")
    parser.add_argument("--page", type=int, default=None, help="1-based page index for tables mode")
    parser.add_argument(
        "--left-source",
        choices=["pdfplumber", "pymupdf", "unstructured"],
        default="pdfplumber",
        help="Source for first-column restoration in tables_firstcol mode",
    )
    parser.add_argument("--out", help="Output text file path", default="tmp/extracted.txt")
    args = parser.parse_args()

    pdf_bytes = read_pdf_bytes(args.url, args.pdf_path)
    out_path = Path(args.out)

    if args.method == "all":
        for method in ["basic", "layout", "words"]:
            text = extract_pdf_text(pdf_bytes, method)
            method_out = out_path.with_suffix(f".{method}.txt")
            write_text(method_out, text)
            print(f"[OK] {method_out}")
        tables_text = extract_tables(pdf_bytes, page_index_1based=args.page)
        tables_out = out_path.with_suffix(".tables.txt")
        write_text(tables_out, tables_text)
        print(f"[OK] {tables_out}")
        firstcol_text = extract_tables_with_first_col_restore(
            pdf_bytes,
            page_index_1based=args.page,
            left_source=args.left_source,
        )
        firstcol_out = out_path.with_suffix(".tables_firstcol.txt")
        write_text(firstcol_out, firstcol_text)
        print(f"[OK] {firstcol_out}")
        return

    if args.method == "tables":
        text = extract_tables(pdf_bytes, page_index_1based=args.page)
        write_text(out_path, text)
        print(f"[OK] {out_path}")
        return

    if args.method == "tables_firstcol":
        text = extract_tables_with_first_col_restore(
            pdf_bytes,
            page_index_1based=args.page,
            left_source=args.left_source,
        )
        write_text(out_path, text)
        print(f"[OK] {out_path}")
        return

    text = extract_pdf_text(pdf_bytes, args.method)
    write_text(out_path, text)
    print(f"[OK] {out_path}")


if __name__ == "__main__":
    main()
