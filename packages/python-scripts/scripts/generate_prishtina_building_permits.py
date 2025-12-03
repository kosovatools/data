#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from openpyxl import load_workbook

SOURCE_URLS: Dict[int, str] = {
    2025: "https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2025%20%283%29.xlsx",
    2024: "https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2024%20%2810%29.xlsx",
    2023: "https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2023%20%281%29.xlsx",
    2022: "https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_2022_23.03.2023.xlsx",
    2021: "https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2021_13.12.2021.xlsx",
    2020: "https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2020_final%20%281%29.xlsx",
    2019: "https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2019%20%2811%29.xlsx",
    2018: "https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2018%20%283%29.xlsx",
    2017: "https://prishtinaonline.com/uploads/lista-e-lejeve-te-leshuara-per-vitin-2017%20%286%29.xlsx",
    2016: "https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2016%20%2816%29.xlsx",
    2015: "https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2015%20%288%29.xlsx",
    2014: "https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2014.xlsx",
    2013: "https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2013.xlsx",
    2012: "https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2012.xlsx",
}

HEADER_ALIASES: Dict[str, str] = {
    "#": "permit_number",
    "data e aplikimit te lejes": "application_date",
    "data e leshimit te lejes": "issuance_date",
    "pronari pronaret perfaqesuesi": "owner",
    "kompania investitori": "investor",
    "projektuesi": "designer",
    "lagja": "neighbourhood",
    "lagjia": "neighbourhood",
    "lagjia e": "neighbourhood",
    "siperfaqja totale ndertimore": "total_floor_area_m2",
    "pagesa totale e lejes se leshuar": "total_fee_eur",
    "etazhiteti": "storeys",
    "etazhiteti i objektit": "storeys",
    "destinimi i objektit": "destination",
    "koment": "comment",
    "dokumenti ne pdf i lejes se leshuar": "document_reference",
    "situacioni i ndertimit": "situation_reference",
    "situacioni": "situation_reference",
}

TEXT_FIELDS = {
    "permit_number",
    "owner",
    "investor",
    "designer",
    "neighbourhood",
    "storeys",
    "destination",
    "document_reference",
}

NUMERIC_FIELDS = {
    "total_floor_area_m2",
    "density_fee_eur",
    "administrative_fee_eur",
    "total_fee_eur",
}

DATE_FIELDS = {"application_date", "issuance_date"}

FIELD_ORDER = [
    "permit_number",
    "application_date",
    "issuance_date",
    "owner",
    "investor",
    "designer",
    "neighbourhood",
    "total_floor_area_m2",
    "density_fee_eur",
    "administrative_fee_eur",
    "total_fee_eur",
    "storeys",
    "destination",
    "document_reference",
    "document_url",
    "situation_url",
]

SMALL_WORDS = {
    "e",
    "dhe",
    "me",
    "nga",
    "ne",
    "në",
    "te",
    "të",
    "per",
    "për",
    "prej",
    "së",
    "se",
    "i",
}
WORD_BOUNDARY = re.compile(r"\b\w+\b", flags=re.UNICODE)


@dataclass
class WorkbookParseResult:
    year: int
    sheet_name: str
    header_row: int  # Excel row number (1-indexed)
    columns: List[str]
    records: List[Dict[str, Any]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate JSON exports for Prishtina building permits."
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("raw_data/prishtina"),
        help="Directory containing building-permits-<year>.xlsx files.",
    )
    parser.add_argument(
        "--pattern",
        default="building-permits-*.xlsx",
        help="Glob pattern for Excel workbooks inside --raw-dir.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/prishtina/building_permits"),
        help="Directory that will receive the JSON exports.",
    )
    return parser.parse_args()


def normalize_header(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    decomposed = unicodedata.normalize("NFKD", text.lower())
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    stripped = stripped.replace("º", "")
    stripped = re.sub(r"[^a-z0-9 ]+", " ", stripped)
    return re.sub(r"\s+", " ", stripped).strip()


def clean_text(value: Any) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[\n]+", ", ", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r",\s*,", ", ", text)
    text = re.sub(r"\s+,", ",", text)
    text = re.sub(r",\s+", ", ", text)
    text = re.sub(r"''+", "'", text)
    text = re.sub(r'""+', '"', text)
    text = re.sub(r"'(?=\s)", "", text)
    text = re.sub(r'"(?=\s)', "", text)
    text = text.strip(" ,\"'")
    return text or None


def smart_title_case(text: str) -> str:
    lowered = text.lower()

    def repl(match: re.Match[str]) -> str:
        word = match.group(0)
        if word in SMALL_WORDS:
            return word
        return word[:1].upper() + word[1:]

    titled = WORD_BOUNDARY.sub(repl, lowered)
    if titled:
        titled = titled[0].upper() + titled[1:]
    return titled


def normalize_inline_separators(
    value: Any, *, dash_separator: Optional[str] = " - ", title_case: bool = False
) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"-\s*,", "-", text)
    text = re.sub(r",\s*-", "-", text)
    if dash_separator is None:
        text = re.sub(r"-+", " ", text)
    else:
        text = re.sub(r"\s*-\s*", dash_separator, text)
    text = re.sub(r"\s*/\s*", " / ", text)
    text = re.sub(r"\s+", " ", text)
    text = text.strip(" ,-/")
    if not text:
        return None
    if title_case:
        text = smart_title_case(text)
    return text or None


def normalize_destination(value: Any) -> Optional[str]:
    return normalize_inline_separators(value, dash_separator=" - ", title_case=True)


def normalize_neighbourhood(value: Any) -> Optional[str]:
    return normalize_inline_separators(value, dash_separator=" - ", title_case=True)


def normalize_document_reference(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    text = re.sub(r"^leja dokumenti[:\s-]+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^(leja|leje)\s+me\s+nr\.?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^(leja|leje)\s+nr\.?\s*", "", text, flags=re.IGNORECASE)
    text = text.lstrip(":- ")
    return text or None


def parse_decimal(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    text = str(value)
    if not text:
        return None
    text = text.replace("€", "").replace("EUR", "")
    text = text.replace(" ", "")
    text = text.replace(",", ".")
    if text.count(".") > 1:
        head, tail = text.rsplit(".", 1)
        head = head.replace(".", "")
        text = f"{head}.{tail}"
    try:
        number = float(text)
    except ValueError:
        return None
    rounded = round(number, 2)
    if rounded == -0.0:
        rounded = 0.0
    return rounded


def parse_date(value: Any) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    ts = pd.to_datetime(value, errors="coerce", dayfirst=True)
    if pd.isna(ts):
        return None
    return ts.date().isoformat()


def discover_files(raw_dir: Path, pattern: str) -> List[Path]:
    files = sorted(raw_dir.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No Excel files matching pattern {pattern} in {raw_dir}")
    return files


def extract_year(path: Path) -> int:
    match = re.search(r"(\d{4})", path.stem)
    if not match:
        raise ValueError(f"Unable to extract year from filename: {path.name}")
    return int(match.group(1))


def find_header_row(frame: pd.DataFrame) -> int:
    for idx, row in frame.iterrows():
        for cell in row.tolist():
            normalized = normalize_header(cell)
            if normalized.startswith("data e leshimit"):
                return idx
    raise ValueError("Unable to locate the header row (missing 'Data e lëshimit të lejes').")


def excel_column_letter(index_zero_based: int) -> str:
    if index_zero_based < 0:
        raise ValueError("Column index must be non-negative")
    result = ""
    idx = index_zero_based
    while idx >= 0:
        idx, remainder = divmod(idx, 26)
        result = chr(ord("A") + remainder) + result
        idx -= 1
    return result


def header_to_field(header: str) -> Optional[str]:
    normalized = normalize_header(header)
    if not normalized:
        return None
    if normalized.startswith("pagesa e tarifes per rritjen e densitetit"):
        return "density_fee_eur"
    if normalized.startswith("pagesa e takses administrative"):
        return "administrative_fee_eur"
    if normalized.startswith("siperfaqja totale ndertimore"):
        return "total_floor_area_m2"
    return HEADER_ALIASES.get(normalized)


def load_dataframe(path: Path) -> Tuple[pd.DataFrame, int, List[str], List[int]]:
    preview = pd.read_excel(path, sheet_name=0, header=None, dtype=object)
    header_idx = find_header_row(preview)
    header_row = preview.iloc[header_idx].tolist()

    columns: List[str] = []
    positions: List[int] = []
    for idx, value in enumerate(header_row):
        label = clean_text(value) if isinstance(value, str) else None
        if idx == 0:
            columns.append("#")
            positions.append(idx)
            continue
        if not label:
            continue
        columns.append(label)
        positions.append(idx)

    data = preview.iloc[header_idx + 1 :, positions].copy()
    data.columns = columns
    data = data.dropna(how="all")
    return data, header_idx, columns, positions


def extract_hyperlink(sheet, column_index: Optional[int], excel_row: int) -> Optional[str]:
    if column_index is None:
        return None
    column_letter = excel_column_letter(column_index)
    cell = sheet[f"{column_letter}{excel_row}"]
    if cell.hyperlink and cell.hyperlink.target:
        return cell.hyperlink.target.strip()
    value = cell.value
    if isinstance(value, str) and value.strip().lower().startswith("http"):
        return value.strip()
    return None


def build_record(row: pd.Series, *, comment_as_destination: bool) -> Dict[str, Any]:
    comment_value = clean_text(row.get("comment"))
    record: Dict[str, Any] = {}
    for field in FIELD_ORDER:
        if field in {"document_url", "situation_url"}:
            # filled separately
            continue
        value = row.get(field)
        if field in TEXT_FIELDS:
            if field == "destination":
                record[field] = normalize_destination(value)
            elif field == "neighbourhood":
                record[field] = normalize_neighbourhood(value)
            elif field == "document_reference":
                record[field] = normalize_document_reference(value)
            else:
                record[field] = clean_text(value)
        elif field in NUMERIC_FIELDS:
            record[field] = parse_decimal(value)
        elif field in DATE_FIELDS:
            record[field] = parse_date(value)
        else:
            record[field] = clean_text(value)
    if comment_as_destination and not record.get("destination") and comment_value:
        destination = normalize_destination(comment_value)
        if destination:
            record["destination"] = destination
    return record


def parse_workbook(path: Path) -> WorkbookParseResult:
    data, header_idx, columns, positions = load_dataframe(path)
    column_map: Dict[str, str] = {}
    pdf_col_excel: Optional[int] = None
    situation_col_excel: Optional[int] = None

    for idx, column in enumerate(columns):
        if column == "#":
            field = "permit_number"
        else:
            field = header_to_field(column)
        if field:
            column_map[column] = field
        normalized = normalize_header(column)
        if pdf_col_excel is None and "pdf" in normalized:
            pdf_col_excel = positions[idx]
        if situation_col_excel is None and normalized.startswith("situacioni"):
            situation_col_excel = positions[idx]

    destination_present = "destination" in column_map.values()
    renamed = data.rename(columns=column_map)
    wb = load_workbook(path, data_only=True, read_only=False)
    sheet = wb.active
    records: List[Dict[str, Any]] = []

    for row_idx, row in renamed.iterrows():
        if row.isna().all():
            continue
        permit_value = clean_text(row.get("permit_number"))
        if not permit_value:
            continue
        record = build_record(row, comment_as_destination=not destination_present)
        excel_row_number = row_idx + 1  # pandas index is zero-based
        record["document_url"] = extract_hyperlink(sheet, pdf_col_excel, excel_row_number)
        record["situation_url"] = extract_hyperlink(
            sheet, situation_col_excel, excel_row_number
        )
        records.append(record)

    wb.close()

    # Preserve workbook order
    year = extract_year(path)
    return WorkbookParseResult(
        year=year,
        sheet_name=sheet.title,
        header_row=header_idx + 1,
        columns=columns,
        records=records,
    )


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    print(f"Wrote {path}")


def main() -> None:
    args = parse_args()
    files = discover_files(args.raw_dir, args.pattern)
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    summary: List[Dict[str, Any]] = []
    for excel_path in files:
        result = parse_workbook(excel_path)
        output_name = f"permits_{result.year}.json"
        payload = {
            "year": result.year,
            "generated_at": generated_at,
            "source_url": SOURCE_URLS.get(result.year),
            "record_count": len(result.records),
            "records": result.records,
        }
        out_file = args.output / output_name
        write_json(out_file, payload)
        records_file = out_file.relative_to(args.output)
        summary.append(
            {
                "year": result.year,
                "records_file": records_file.as_posix(),
                "record_count": len(result.records),
            }
        )

    summary.sort(key=lambda item: item["year"], reverse=True)
    write_json(
        args.output / "index.json",
        {
            "generated_at": generated_at,
            "years": summary,
        },
    )


if __name__ == "__main__":
    main()
