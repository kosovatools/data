#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
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
    return sorted(p for p in source_dir.rglob("*.xlsx") if p.is_file())


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
    cleaned["city"] = cleaned["city"].astype(str).str.strip()
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


def to_records(frame: pd.DataFrame, sort_columns: Iterable[str] | None = None) -> list[dict]:
    if sort_columns:
        frame = frame.sort_values(list(sort_columns))
    return [
        {key: (value.item() if hasattr(value, "item") else value) for key, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def build_outputs(dataset: pd.DataFrame, output_dir: Path) -> None:
    dataset = dataset.copy()
    last_year = int(dataset["year"].max())
    last_year_data = dataset[dataset["year"] == last_year]

    monthly_category_city = (
        last_year_data.groupby(["month", "category", "city"], as_index=False)
        .agg(turnover=("turnover", "sum"), taxpayers=("taxpayers", "sum"))
    )
    write_json(
        output_dir / "monthly_category_city_last_year.json",
        {
            "year": last_year,
            "records": to_records(monthly_category_city, sort_columns=["month", "category", "city"]),
        },
    )

    categories_last_year = (
        last_year_data.groupby("category", as_index=False)
        .agg(turnover=("turnover", "sum"), taxpayers=("taxpayers", "sum"))
        .sort_values("turnover", ascending=False)
    )
    write_json(
        output_dir / "categories_last_year.json",
        {
            "year": last_year,
            "records": to_records(categories_last_year),
        },
    )

    cities_last_year = (
        last_year_data.groupby("city", as_index=False)
        .agg(turnover=("turnover", "sum"), taxpayers=("taxpayers", "sum"))
        .sort_values("turnover", ascending=False)
    )
    write_json(
        output_dir / "cities_last_year.json",
        {
            "year": last_year,
            "records": to_records(cities_last_year),
        },
    )

    grouped = (
        dataset.groupby(["year", "city", "category"], as_index=False)
        .agg(turnover=("turnover", "sum"), taxpayers=("taxpayers", "sum"))
    )
    top_category_by_city_year = (
        grouped.sort_values(["year", "city", "turnover"], ascending=[True, True, False])
        .groupby(["year", "city"], group_keys=False)
        .head(8)
        .copy()
    )
    top_category_by_city_year["rank"] = top_category_by_city_year.groupby(["year", "city"]).cumcount() + 1
    write_json(
        output_dir / "top_category_by_city_over_years.json",
        to_records(top_category_by_city_year, sort_columns=["year", "city", "rank"]),
    )

    categories_over_years = (
        grouped.groupby(["year", "category"], as_index=False)
        .agg(turnover=("turnover", "sum"), taxpayers=("taxpayers", "sum"))
    )
    write_json(
        output_dir / "categories_over_years.json",
        to_records(categories_over_years, sort_columns=["year", "category"]),
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
