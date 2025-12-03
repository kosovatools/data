#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

import pandas as pd

ACCREDITED_SOURCES = [
    {
        "file": "Tabela-Programet-e-Akredituara-2025-2026_v5.xlsx",
        "source_url": "https://akreditimi.rks-gov.net/wp-content/uploads/Tabela-Programet-e-Akredituara-2025-2026_v5.xlsx",
        "output": ("accredited_programmes", "programmes_2025_2026.json"),
        "period": "2025-2026",
        "version": "v5"
    },
    {
        "file": "AKA-KAA-Proggramet-e-Akredituara-2023-2024.xlsx",
        "source_url": "https://akreditimi.rks-gov.net/wp-content/uploads/2023/08/AKA-KAA-Proggramet-e-Akredituara-2023-2024.xlsx",
        "output": ("accredited_programmes", "programmes_2023_2024.json"),
        "period": "2023-2024"
    }
]

ACCREDITED_HEADER_MAP = {
    "institucioni i arsimit te larte": "institution",
    "institucioni i arsimit te larte higher education institution": "institution",
    "nr": "program_number",
    "programi i studimit": "programme_sq",
    "progarmi i studimit": "programme_sq",
    "programet e studimit": "programme_sq",
    "study program": "programme_en",
    "kampusi": "campus",
    "niveli": "level",
    "ects": "ects",
    "kuota": "quota",
    "i akredituar deri me": "accredited_until",
    "i akredituar deri më": "accredited_until"
}

ACCREDITED_FIELD_ORDER = [
    "institution",
    "program_number",
    "programme_sq",
    "programme_en",
    "campus",
    "level",
    "ects",
    "quota",
    "accredited_until"
]

YEAR_PATTERN = re.compile(r"(20\d{2})")


def normalize_header(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("\n", " ")
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def clean_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    return re.sub(r"\s+", " ", text)


def to_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        num = int(round(float(value)))
    except (TypeError, ValueError, OverflowError):
        return None
    return num


def to_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return round(num, 2)


def format_date(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.date().isoformat()


def extract_year(text: str) -> int | None:
    match = YEAR_PATTERN.search(text)
    if match:
        return int(match.group(1))
    return None


def dataset_index_key(item: Dict[str, Any]) -> tuple:
    period_year = extract_year(item.get("period", "")) or 0
    # Sort by coverage year (latest first), then category/path for stability
    return (-period_year, item.get("category"), item.get("path"))


def ensure_file(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"Missing required source file: {path}")
    return path


def load_accredited_dataframe(path: Path) -> pd.DataFrame:
    preview = pd.read_excel(path, sheet_name=0, header=None, dtype=object)
    header_row: int | None = None
    for idx, row in preview.iterrows():
        first_cell = clean_text(row[0])
        if first_cell and "institucioni" in first_cell.lower():
            header_row = idx
            break
    if header_row is None:
        raise ValueError(f"Unable to locate the header row in {path.name}")
    return pd.read_excel(path, sheet_name=0, header=header_row, dtype=object)


def parse_accredited_programmes(path: Path) -> List[Dict[str, Any]]:
    df = load_accredited_dataframe(path)
    rename_map: Dict[str, str] = {}
    for column in df.columns:
        normalized = normalize_header(column)
        target = ACCREDITED_HEADER_MAP.get(normalized)
        if target:
            rename_map[column] = target

    if "institution" not in rename_map.values():
        raise ValueError(f"Unable to identify institution column in {path.name}")

    df = df.rename(columns=rename_map)
    missing_fields = [field for field in ACCREDITED_FIELD_ORDER if field not in df.columns]
    if missing_fields:
        raise ValueError(f"{path.name} is missing columns: {', '.join(missing_fields)}")

    df = df[ACCREDITED_FIELD_ORDER]
    df["institution"] = df["institution"].ffill()
    df = df.dropna(subset=["programme_sq", "programme_en"], how="all")
    df = df.dropna(subset=["institution"])

    records: List[Dict[str, Any]] = []
    for row in df.itertuples(index=False):
        record = {
            "institution": clean_text(row.institution),
            "program_number": to_int(row.program_number),
            "programme_sq": clean_text(row.programme_sq),
            "programme_en": clean_text(row.programme_en),
            "campus": clean_text(row.campus),
            "level": clean_text(row.level),
            "ects": to_int(row.ects),
            "quota": to_int(row.quota),
            "accredited_until": format_date(row.accredited_until)
        }
        if not record["programme_sq"] and not record["programme_en"]:
            continue
        records.append(record)

    return records

def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    print(f"Wrote {path}")


def build_accredited_payload(
    generated_at: str, source: Dict[str, Any], records: Sequence[Dict[str, Any]]
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "generated_at": generated_at,
        "period": source["period"],
        "record_count": len(records),
        "institution_count": len({r["institution"] for r in records if r["institution"]}),
        "source_url": source["source_url"],
        "source_file": source["source_file"],
        "records": records
    }
    if "version" in source:
        payload["version"] = source["version"]
    return payload


def generate_datasets(raw_dir: Path, output_dir: Path) -> None:
    timestamp = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
    index_entries: List[Dict[str, Any]] = []

    for source in ACCREDITED_SOURCES:
        source_path = ensure_file(raw_dir / source["file"])
        records = parse_accredited_programmes(source_path)
        payload = build_accredited_payload(
            timestamp,
            {**source, "source_file": str(source_path)},
            records
        )
        output = output_dir / source["output"][0] / source["output"][1]
        write_json(output, payload)

        relative_output = Path(source["output"][0]) / source["output"][1]
        entry: Dict[str, Any] = {
            "category": source["output"][0],
            "path": relative_output.as_posix(),
            "period": payload["period"],
            "record_count": payload["record_count"],
            "institution_count": payload["institution_count"],
            "source_url": payload["source_url"],
            "source_file": payload["source_file"],
            "generated_at": payload["generated_at"]
        }
        if "version" in payload:
            entry["version"] = payload["version"]
        index_entries.append(entry)

    index_entries.sort(key=dataset_index_key)
    write_json(
        output_dir / "index.json",
        {
            "generated_at": timestamp,
            "datasets": index_entries
        }
    )

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate KAA datasets from Excel workbooks."
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("raw_data/me"),
        help="Directory containing the downloaded KAA Excel sources."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/kaa"),
        help="Directory where the derived JSON datasets should be written."
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    generate_datasets(args.raw_dir, args.output)


if __name__ == "__main__":
    main()
