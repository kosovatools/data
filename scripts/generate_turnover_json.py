#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
import unicodedata
from typing import Any, Iterable

import pandas as pd


HEADER_KEYWORDS = {
    "year": ("year", "viti", "godina"),
    "month": ("month", "muaji", "mesec"),
    "category": ("kategori", "sektor", "description"),
    "city": ("komuna", "municipality", "opština"),
    "registration_status": ("registration", "status"),
    "taxpayers": ("number of taxpayers", "tatimpaguesve", "poreskih obveznika"),
    "turnover": ("turnover", "qarkullim", "promet"),
}


def discover_excel_files(source_dir: Path) -> list[Path]:
    return sorted(
        p
        for p in source_dir.rglob("*.xlsx")
        if p.is_file() and "turnover" in p.name.lower()
    )


def detect_header_row(frame: pd.DataFrame) -> int:
    for idx, row in frame.iterrows():
        for cell in row:
            if isinstance(cell, str):
                lowered = cell.lower()
                if "year" in lowered or "viti" in lowered:
                    return idx
    raise ValueError("Failed to locate header row containing year/month information.")


def normalise_column_name(cell_value: object) -> str | None:
    if not isinstance(cell_value, str):
        return None
    lowered = cell_value.lower()
    for canonical, keywords in HEADER_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return canonical
    return None


def load_turnover_data(excel_path: Path) -> pd.DataFrame:
    raw = pd.read_excel(excel_path, sheet_name=0, header=None)
    header_row_idx = detect_header_row(raw)
    header_values = raw.iloc[header_row_idx]
    data = raw.iloc[header_row_idx + 1 :].copy()
    data.columns = header_values

    rename_map = {col: normalise_column_name(col) for col in data.columns}

    selected_columns = {
        name: new_name
        for name, new_name in rename_map.items()
        if new_name in {"year", "month", "category", "city", "registration_status", "taxpayers", "turnover"}
    }

    cleaned = (
        data[list(selected_columns.keys())]
        .rename(columns=selected_columns)
        .dropna(how="all")
    )

    cleaned["year"] = pd.to_numeric(cleaned["year"], errors="coerce").astype("Int64")
    cleaned["month"] = pd.to_numeric(cleaned["month"], errors="coerce").astype("Int64")
    cleaned["taxpayers"] = pd.to_numeric(cleaned.get("taxpayers"), errors="coerce")
    cleaned["turnover"] = pd.to_numeric(cleaned.get("turnover"), errors="coerce")

    cleaned["category"] = cleaned["category"].astype(str).str.strip()
    cleaned["city"] = cleaned["city"].apply(format_city_label)
    if "registration_status" in cleaned.columns:
        cleaned["registration_status"] = cleaned["registration_status"].astype(str).str.strip()

    cleaned = cleaned.dropna(subset=["year", "month", "category", "city", "turnover"])
    cleaned["year"] = cleaned["year"].astype(int)
    cleaned["month"] = cleaned["month"].astype(int)
    cleaned["taxpayers"] = cleaned["taxpayers"].fillna(0).round().astype(int)
    cleaned["turnover"] = cleaned["turnover"].astype(float)

    cleaned = cleaned[(cleaned["category"] != "") & (cleaned["city"] != "")]
    aggregate_tokens = {"total", "totali"}
    cleaned = cleaned[
        ~cleaned["category"].str.lower().isin(aggregate_tokens)
        & ~cleaned["city"].str.lower().isin(aggregate_tokens)
    ]

    cleaned["source_file"] = excel_path.name
    return cleaned.reset_index(drop=True)


def gather_turnover_frames(files: Iterable[Path]) -> pd.DataFrame:
    frames = []
    for file_path in files:
        frame = load_turnover_data(file_path)
        frame["source_year"] = frame["year"]
        filename_year = extract_year_from_filename(file_path)
        if filename_year is not None:
            frame["source_year"] = filename_year
        frames.append(frame)
    if not frames:
        raise ValueError("No Excel data files were discovered. Populate the source directory first.")
    return pd.concat(frames, ignore_index=True)


def extract_year_from_filename(path: Path) -> int | None:
    match = re.search(r"(20\d{2})", path.stem)
    if not match:
        return None
    return int(match.group(1))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def format_currency(value: float) -> float:
    return round(float(value), 2)


def format_city_label(city: Any) -> str:
    if pd.isna(city):
        return ""
    normalized = str(city).strip()
    if not normalized:
        return ""
    lowered = re.sub(r"\s+", " ", normalized.lower())
    if lowered in {"nan", "none"}:
        return ""
    return lowered.title()


def as_int(value: Any) -> int:
    return int(round(float(value)))


def slugify_label(label: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(label).strip())
    ascii_safe = normalized.encode("ascii", "ignore").decode("ascii").lower()
    slug = re.sub(r"[^\w]+", "_", ascii_safe)
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug


def build_slug_map(options: Iterable[str]) -> dict[str, str]:
    slug_counts: dict[str, int] = {}
    mapping: dict[str, str] = {}
    for label in sorted(set(options), key=lambda value: value.lower()):
        base = slugify_label(label) or "item"
        count = slug_counts.get(base, 0)
        slug = base if count == 0 else f"{base}_{count}"
        while slug in mapping.values():
            count += 1
            slug = f"{base}_{count}"
        slug_counts[base] = count + 1
        mapping[label] = slug
    return mapping


def build_dimension_options(mapping: dict[str, str]) -> list[dict[str, str]]:
    return [
        {"key": slug, "label": label}
        for label, slug in sorted(mapping.items(), key=lambda item: item[0].lower())
    ]


def build_time(periods: list[str], granularity: str) -> dict[str, Any]:
    if not periods:
        raise ValueError("Dataset is missing period information.")
    return {
        "key": "period",
        "granularity": granularity,
        "first": periods[0],
        "last": periods[-1],
        "count": len(periods),
    }


def build_outputs(dataset: pd.DataFrame, output_dir: Path) -> None:
    dataset = dataset.copy()
    years = sorted(int(value) for value in dataset["year"].unique())
    if not years:
        raise ValueError("No turnover data found in the provided Excel exports.")

    last_year = years[-1]
    timestamp = iso_timestamp()
    category_slug_map = build_slug_map(dataset["category"].unique())
    city_slug_map = build_slug_map(dataset["city"].unique())
    dataset["category_slug"] = dataset["category"].map(category_slug_map)
    dataset["city_slug"] = dataset["city"].map(city_slug_map)
    dimension_categories = build_dimension_options(category_slug_map)
    dimension_cities = build_dimension_options(city_slug_map)

    # Categories × Year
    categories_yearly = (
        dataset.groupby(["year", "category_slug"], as_index=False)
        .agg(turnover=("turnover", "sum"), taxpayers=("taxpayers", "sum"))
        .sort_values(["year", "category_slug"])
    )
    category_records: list[dict[str, Any]] = []
    for row in categories_yearly.to_dict(orient="records"):
        category_records.append(
            {
                "period": str(int(row["year"])),
                "category": row["category_slug"],
                "turnover": format_currency(row["turnover"]),
                "taxpayers": as_int(row["taxpayers"]),
            }
        )
    year_periods = [str(year) for year in years]
    category_meta = {
        "id": "mfk_turnover_categories_yearly",
        "title": "Qarkullimi sipas kategorive (vjetor)",
        "generated_at": timestamp,
        "updated_at": None,
        "source": "Ministria e Financave, Punës dhe Transfereve (MFK)",
        "source_urls": ["https://mfpt.rks-gov.net"],
        "time": build_time(year_periods, "yearly"),
        "fields": [
            {"key": "turnover", "label": "Qarkullimi", "unit": "EUR"},
            {"key": "taxpayers", "label": "Tatimpagues", "unit": "count"},
        ],
        "metrics": ["turnover", "taxpayers"],
        "dimensions": {"category": dimension_categories},
        "extras": {"currency": "EUR"},
    }
    write_json(
        output_dir / "mfk_turnover_categories_yearly.json",
        {"meta": category_meta, "records": category_records},
    )

    # Cities × Year
    cities_yearly = (
        dataset.groupby(["year", "city_slug"], as_index=False)
        .agg(turnover=("turnover", "sum"), taxpayers=("taxpayers", "sum"))
        .sort_values(["year", "city_slug"])
    )
    city_records: list[dict[str, Any]] = []
    for row in cities_yearly.to_dict(orient="records"):
        city_records.append(
            {
                "period": str(int(row["year"])),
                "city": row["city_slug"],
                "turnover": format_currency(row["turnover"]),
                "taxpayers": as_int(row["taxpayers"]),
            }
        )
    city_meta = {
        "id": "mfk_turnover_cities_yearly",
        "title": "Qarkullimi sipas komunave (vjetor)",
        "generated_at": timestamp,
        "updated_at": None,
        "source": "Ministria e Financave, Punës dhe Transfereve (MFK)",
        "source_urls": ["https://mfpt.rks-gov.net"],
        "time": build_time(year_periods, "yearly"),
        "fields": [
            {"key": "turnover", "label": "Qarkullimi", "unit": "EUR"},
            {"key": "taxpayers", "label": "Tatimpagues", "unit": "count"},
        ],
        "metrics": ["turnover", "taxpayers"],
        "dimensions": {"city": dimension_cities},
        "extras": {"currency": "EUR"},
    }
    write_json(
        output_dir / "mfk_turnover_cities_yearly.json",
        {"meta": city_meta, "records": city_records},
    )

    # City × Category × Year rankings
    grouped = (
        dataset.groupby(["year", "city_slug", "category_slug"], as_index=False)
        .agg(turnover=("turnover", "sum"), taxpayers=("taxpayers", "sum"))
    )
    rankings = (
        grouped.sort_values(["year", "city_slug", "turnover"], ascending=[True, True, False])
        .groupby(["year", "city_slug"], group_keys=False)
        .head(8)
        .copy()
    )
    rankings["rank"] = rankings.groupby(["year", "city_slug"]).cumcount() + 1
    ranking_records: list[dict[str, Any]] = []
    for row in rankings.sort_values(["year", "city_slug", "rank"]).to_dict(orient="records"):
        ranking_records.append(
            {
                "period": str(int(row["year"])),
                "city": row["city_slug"],
                "category": row["category_slug"],
                "turnover": format_currency(row["turnover"]),
                "taxpayers": as_int(row["taxpayers"]),
                "rank": int(row["rank"]),
            }
        )
    ranking_meta = {
        "id": "mfk_turnover_city_category_yearly",
        "title": "Top kategoritë sipas komunave (vjetor)",
        "generated_at": timestamp,
        "updated_at": None,
        "source": "Ministria e Financave, Punës dhe Transfereve (MFK)",
        "source_urls": ["https://mfpt.rks-gov.net"],
        "time": build_time(year_periods, "yearly"),
        "fields": [
            {"key": "turnover", "label": "Qarkullimi", "unit": "EUR"},
            {"key": "taxpayers", "label": "Tatimpagues", "unit": "count"},
            {"key": "rank", "label": "Renditja", "unit": "index"},
        ],
        "metrics": ["turnover", "taxpayers"],
        "dimensions": {"city": dimension_cities, "category": dimension_categories},
        "extras": {"currency": "EUR"},
    }
    write_json(
        output_dir / "mfk_turnover_city_category_yearly.json",
        {"meta": ranking_meta, "records": ranking_records},
    )

    # Monthly Categories × City (latest year)
    last_year_data = dataset[dataset["year"] == last_year]
    monthly = (
        last_year_data.groupby(["month", "category_slug", "city_slug"], as_index=False)
        .agg(turnover=("turnover", "sum"), taxpayers=("taxpayers", "sum"))
        .sort_values(["month", "category_slug", "city_slug"])
    )
    monthly_records: list[dict[str, Any]] = []
    periods = sorted(
        {f"{last_year}-{int(month):02d}" for month in last_year_data["month"].unique()}
    )
    for row in monthly.to_dict(orient="records"):
        period = f"{last_year}-{int(row['month']):02d}"
        monthly_records.append(
            {
                "period": period,
                "category": row["category_slug"],
                "city": row["city_slug"],
                "turnover": format_currency(row["turnover"]),
                "taxpayers": as_int(row["taxpayers"]),
            }
        )
    monthly_meta = {
        "id": "mfk_turnover_city_category_monthly",
        "title": "Qarkullimi mujor sipas kategorive dhe komunave",
        "generated_at": timestamp,
        "updated_at": None,
        "source": "Ministria e Financave, Punës dhe Transfereve (MFK)",
        "source_urls": ["https://mfpt.rks-gov.net"],
        "time": build_time(periods, "monthly"),
        "fields": [
            {"key": "turnover", "label": "Qarkullimi", "unit": "EUR"},
            {"key": "taxpayers", "label": "Tatimpagues", "unit": "count"},
        ],
        "metrics": ["turnover", "taxpayers"],
        "dimensions": {"city": dimension_cities, "category": dimension_categories},
        "extras": {"currency": "EUR", "coverage_year": last_year},
    }
    write_json(
        output_dir / "mfk_turnover_city_category_monthly.json",
        {"meta": monthly_meta, "records": monthly_records},
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform turnover Excel exports into JSON summaries.")
    parser.add_argument(
        "--source",
        type=Path,
        default=Path("raw_data"),
        help="Directory containing the turnover Excel files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/mfk/turnover"),
        help="Directory where JSON outputs will be written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    excel_files = discover_excel_files(args.source)
    dataset = gather_turnover_frames(excel_files)
    build_outputs(dataset, args.output)


if __name__ == "__main__":
    main()
