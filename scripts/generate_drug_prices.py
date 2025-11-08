#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from openpyxl import load_workbook


COLUMN_MAP = {
    "Nr rendor": "serial_number",
    "Emri i produktit": "product_name",
    "Substanca Aktive": "active_substance",
    "ATC Kodi": "atc_code",
    "Doza": "dose",
    "Forma Farmaceutike": "pharmaceutical_form",
    "Paketimi": "packaging",
    "Mbajtësi i AM": "marketing_authorisation_holder",
    "Prodhuesi": "manufacturer",
    "Numri i MA/RMA/PMA": "authorization_number",
    "ÇMIMI ME SHUMICË": "price_wholesale",
    "ÇMIMI ME MARZHË": "price_with_margin",
    "ÇMIMI ME PAKICË": "price_retail",
    "Data e validitetit": "valid_until",
}

REGION_MAP = {
    "Maqedoni": "macedonia",
    "Mali i zi": "montenegro",
    "Kroaci": "croatia",
    "Slloveni": "slovenia",
    "Bullgari": "bulgaria",
    "Estoni": "estonia",
    "tjeter": "other",
}

DROP_NUMERIC_TOKENS = {
    "",
    "ska",
    "sk",
    "cmim cip",
    "cmim cip +7.5%",
    "referojuni deklarates nga bam",
    "referojuni deklarates nga bam.",
}

DESCRIPTOR_FIELDS = (
    "product_name",
    "active_substance",
    "atc_code",
    "dose",
    "pharmaceutical_form",
    "packaging",
    "marketing_authorisation_holder",
    "manufacturer",
    "authorization_number",
)

STATIC_FIELDS = ("serial_number",) + DESCRIPTOR_FIELDS

PRICE_FIELDS = ("price_wholesale", "price_with_margin", "price_retail")

PRICE_META_FIELDS = ("valid_until", "reference_prices", "reference_prices_secondary")

DEDUPLICATION_FIELDS = (
    "atc_code",
    "authorization_number",
    "product_name",
    "dose",
    "packaging",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate cleaned JSON exports for Ministry of Health drug prices."
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=Path("raw_data"),
        help="Directory containing `drug-prices-*.xlsx` files.",
    )
    parser.add_argument(
        "--pattern",
        default="drug-prices-*.xlsx",
        help="Glob pattern for Excel input files (relative to --source).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/mh/drug_prices"),
        help="Directory that will receive JSON outputs.",
    )
    return parser.parse_args()


def discover_excel_files(source_dir: Path, pattern: str) -> list[Path]:
    return sorted(source_dir.glob(pattern))


def extract_version(excel_path: Path) -> str:
    match = re.search(r"drug-prices-(\d+(?:\.\d+)*)", excel_path.stem)
    if not match:
        raise ValueError(f"Failed to extract version from filename: {excel_path.name}")
    return match.group(1)


def visible_headers(excel_path: Path) -> list[str]:
    workbook = load_workbook(excel_path, read_only=False, data_only=True)
    worksheet = workbook.active
    headers: list[str] = []
    try:
        for cell in worksheet[2]:
            column_letter = cell.column_letter
            if worksheet.column_dimensions[column_letter].hidden:
                continue
            if cell.value is None:
                continue
            headers.append(column_letter)
    finally:
        workbook.close()
    if not headers:
        raise ValueError(f"No visible headers found in {excel_path.name}")
    return headers


def excel_letter_to_index(letter: str) -> int:
    result = 0
    for char in letter:
        result = result * 26 + (ord(char.upper()) - ord("A") + 1)
    return result - 1


def load_visible_frame(excel_path: Path, column_letters: list[str]) -> pd.DataFrame:
    frame = pd.read_excel(excel_path, header=1)
    column_names = frame.columns.tolist()
    selected_names = []
    for letter in column_letters:
        idx = excel_letter_to_index(letter)
        if 0 <= idx < len(column_names):
            selected_names.append(column_names[idx])
    return frame[selected_names]


def to_int(value: Any) -> int | None:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return None
    return int(number)


def clean_text(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    text = str(value).strip()
    return text or None


def normalise_decimal(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    text = str(value).strip().lower()
    if text in DROP_NUMERIC_TOKENS:
        return None
    text = text.replace(" ", "")
    text = text.replace(",", ".")
    if text.count(".") > 1:
        head, tail = text.rsplit(".", 1)
        head = head.replace(".", "")
        text = f"{head}.{tail}"
    try:
        numeric = float(text)
    except ValueError:
        return None
    rounded = round(numeric, 2)
    if rounded == -0.0:
        rounded = 0.0
    return rounded


def parse_validity(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.date().isoformat() if isinstance(value, datetime) else value.isoformat()
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return text


def version_key(version: str) -> tuple[int, ...]:
    return tuple(int(token) for token in version.split("."))


def build_record(row: dict[str, Any], version: str) -> dict[str, Any]:
    record: dict[str, Any] = {"version": version}
    for source, target in COLUMN_MAP.items():
        if source not in row:
            continue
        value = row[source]
        if target == "serial_number":
            record[target] = to_int(value)
        elif target == "valid_until":
            record[target] = parse_validity(value)
        elif target.startswith("price"):
            record[target] = normalise_decimal(value)
        else:
            record[target] = clean_text(value)

    region_primary = {}
    for source, slug in REGION_MAP.items():
        if source in row:
            parsed = normalise_decimal(row[source])
            if parsed is not None:
                region_primary[slug] = parsed
    if region_primary:
        record["reference_prices"] = region_primary

    region_secondary = {}
    for source, slug in REGION_MAP.items():
        secondary_column = f"{source}.1"
        if secondary_column in row:
            parsed = normalise_decimal(row[secondary_column])
            if parsed is not None:
                region_secondary[slug] = parsed
    if region_secondary:
        record["reference_prices_secondary"] = region_secondary

    return record


def record_key(record: dict[str, Any]) -> tuple:
    return tuple(record.get(field) for field in DESCRIPTOR_FIELDS)


def pick_best(value_a: Any, value_b: Any) -> Any:
    return value_a if value_a not in (None, "", []) else value_b


def deduplication_key(record: dict[str, Any]) -> tuple[Any, ...] | None:
    key = tuple(record.get(field) for field in DEDUPLICATION_FIELDS)
    if any(value is None for value in key):
        return None
    return key


def deduplicate_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[tuple[Any, ...], dict[str, Any]] = {}
    ordered: list[dict[str, Any]] = []
    for record in records:
        key = deduplication_key(record)
        if key is None:
            ordered.append(record)
            continue
        existing = unique.get(key)
        if existing is None:
            unique[key] = record
            ordered.append(record)
            continue
        for field, value in record.items():
            existing[field] = pick_best(existing.get(field), value)
    return ordered


def aggregate_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    aggregated: dict[tuple, dict[str, Any]] = {}
    for record in records:
        key = record_key(record)
        if key not in aggregated:
            aggregated[key] = {
                "data": {field: record.get(field) for field in STATIC_FIELDS},
                "latest_version": None,
                "latest_snapshot": {},
                "history": [],
            }
        entry = aggregated[key]
        snapshot = {"version": record["version"]}
        for field in PRICE_FIELDS + PRICE_META_FIELDS:
            value = record.get(field)
            if value is not None:
                snapshot[field] = value
        entry["history"].append(snapshot)
        if (
            entry["latest_version"] is None
            or version_key(record["version"]) >= version_key(entry["latest_version"])
        ):
            entry["latest_version"] = record["version"]
            for field in STATIC_FIELDS:
                existing = entry["data"].get(field)
                entry["data"][field] = pick_best(record.get(field), existing)
            entry["latest_snapshot"] = snapshot

    results: list[dict[str, Any]] = []
    for entry in aggregated.values():
        record_data: dict[str, Any] = {}
        for field in STATIC_FIELDS:
            value = entry["data"].get(field)
            if value is not None:
                record_data[field] = value

        latest_snapshot = entry["latest_snapshot"]
        for field in PRICE_FIELDS + PRICE_META_FIELDS:
            value = latest_snapshot.get(field)
            if value is not None:
                record_data[field] = value

        record_data["latest_version"] = entry["latest_version"]
        history_sorted = sorted(
            entry["history"],
            key=lambda snap: version_key(snap["version"]),
            reverse=True,
        )
        record_data["version_history"] = history_sorted
        results.append(record_data)

    results.sort(key=lambda rec: (rec.get("product_name") or "", rec.get("packaging") or ""))
    return results


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def main() -> None:
    args = parse_args()
    excel_files = discover_excel_files(args.source, args.pattern)
    if not excel_files:
        raise SystemExit("No Excel files matched the provided pattern.")

    master_records: list[dict[str, Any]] = []
    summary: list[dict[str, Any]] = []
    for excel_path in sorted(excel_files, key=lambda path: version_key(extract_version(path))):
        version = extract_version(excel_path)
        letters = visible_headers(excel_path)
        frame = load_visible_frame(excel_path, letters)
        frame = frame.dropna(subset=["Emri i produktit"], how="all")
        version_records = [
            build_record(row, version) for row in frame.to_dict(orient="records")
        ]
        version_records = [record for record in version_records if record.get("product_name")]
        version_records = deduplicate_records(version_records)
        master_records.extend(version_records)
        valid_values = sorted(
            {record["valid_until"] for record in version_records if record.get("valid_until")}
        )
        summary.append(
            {
                "version": version,
                "source_file": str(excel_path),
                "record_count": len(version_records),
                "valid_until_values": valid_values,
            }
        )

    master_records.sort(key=lambda record: (version_key(record["version"]), record.get("serial_number") or 0))
    aggregated_records = aggregate_records(master_records)
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    write_json(
        args.output / "records.json",
        {
            "generated_at": generated_at,
            "records": aggregated_records,
        },
    )
    write_json(
        args.output / "versions.json",
        {
            "generated_at": generated_at,
            "versions": summary,
        },
    )


if __name__ == "__main__":
    main()
