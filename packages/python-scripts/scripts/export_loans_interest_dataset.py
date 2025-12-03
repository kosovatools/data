"""
Build a JSON dataset from raw_data/loans_interest_IntRates_Loans.csv.

Output path: data/cbk/loan_interests.json
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from zipfile import ZipFile
import xml.etree.ElementTree as ET


NS = {
    "d": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}

MONTH_MAP = {
    "Jan": 1,
    "Shk": 2,
    "Mar": 3,
    "Pri": 4,
    "Maj": 5,
    "Qer": 6,
    "Korr": 7,
    "Gush": 8,
    "Shta": 9,
    "Tet": 10,
    "Nën": 11,
    "Nen": 11,
    "Dhj": 12,
}


def normalize_description(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        return ""
    lowered = cleaned.lower()
    return lowered[0].upper() + lowered[1:]


def col_to_index(col: str) -> int:
    n = 0
    for ch in col:
        if ch.isalpha():
            n = n * 26 + (ord(ch.upper()) - 64)
    return n


def get_shared_strings(zf: ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    tree = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    out: List[str] = []
    for si in tree.findall("d:si", NS):
        out.append("".join((t.text or "") for t in si.findall(".//d:t", NS)))
    return out


def get_value(cell: ET.Element, shared: List[str]) -> Optional[str]:
    t = cell.attrib.get("t")
    if t == "s":
        v = cell.find("d:v", NS)
        return shared[int(v.text)] if v is not None else None
    if t == "inlineStr":
        return "".join((t.text or "") for t in cell.findall(".//d:t", NS))
    v = cell.find("d:v", NS)
    return v.text if v is not None else None


def get_sheet_target(zf: ZipFile, sheet_name: str) -> str:
    wb_tree = ET.fromstring(zf.read("xl/workbook.xml"))
    rels_tree = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rels = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rels_tree.findall(
            "d:Relationship",
            {"d": "http://schemas.openxmlformats.org/package/2006/relationships"},
        )
    }
    for sheet in wb_tree.findall("d:sheets/d:sheet", NS):
        if sheet.attrib.get("name") == sheet_name:
            rid = sheet.attrib.get(f"{{{NS['r']}}}id")
            if rid and rid in rels:
                return rels[rid]
    raise ValueError(f"Sheet {sheet_name!r} not found")


def load_from_excel(path: Path, start: datetime) -> Tuple[List[dict], Dict[str, str], Set[str]]:
    records: List[dict] = []
    descriptions: Dict[str, str] = {}
    periods: Set[str] = set()

    with ZipFile(path) as zf:
        shared = get_shared_strings(zf)
        target = get_sheet_target(zf, "IntRates_Loans")
        sheet = ET.fromstring(zf.read(f"xl/{target}"))

        header_row4 = header_row5 = None
        for row in sheet.findall("d:sheetData/d:row", NS):
            rnum = int(row.attrib.get("r", "0"))
            if rnum == 4:
                header_row4 = row
            elif rnum == 5:
                header_row5 = row
            if header_row4 is not None and header_row5 is not None:
                break
        if header_row4 is None or header_row5 is None:
            raise ValueError("Header rows 4 and/or 5 missing in sheet IntRates_Loans")

        def row_to_dict(row: ET.Element) -> Dict[int, Optional[str]]:
            data: Dict[int, Optional[str]] = {}
            for cell in row.findall("d:c", NS):
                ref = cell.attrib.get("r", "")
                col_letters = "".join(c for c in ref if c.isalpha())
                idx = col_to_index(col_letters)
                data[idx] = get_value(cell, shared)
            return data

        headers_year = row_to_dict(header_row4)
        headers_month = row_to_dict(header_row5)

        col_dates: Dict[int, str] = {}
        current_year: Optional[int] = None
        for col in sorted(set(headers_month.keys()) | set(headers_year.keys())):
            y_val = headers_year.get(col)
            if y_val and y_val != "0":
                try:
                    current_year = int(float(y_val))
                except Exception:
                    current_year = None
            month_name = headers_month.get(col)
            if current_year and month_name:
                mnum = MONTH_MAP.get(month_name)
                if mnum:
                    col_dates[col] = f"{current_year:04d}-{mnum:02d}"

        code_pattern = re.compile(r"^[TNH](?:_[0-9A-Za-z]+)*$")

        for row in sheet.findall("d:sheetData/d:row", NS):
            rnum = int(row.attrib.get("r", "0"))
            if rnum < 7:
                continue
            cells: Dict[int, Optional[str]] = {}
            for cell in row.findall("d:c", NS):
                ref = cell.attrib.get("r", "")
                col_letters = "".join(c for c in ref if c.isalpha())
                idx = col_to_index(col_letters)
                cells[idx] = get_value(cell, shared)

            code = cells.get(2)
            desc = normalize_description(cells.get(3, "") or "")
            if not code or code == "0" or not code_pattern.match(code):
                continue
            row_records = []
            for col, period in col_dates.items():
                try:
                    period_dt = datetime.strptime(period, "%Y-%m")
                except Exception:
                    continue
                if period_dt < start:
                    continue
                if col not in cells or cells[col] in (None, ""):
                    continue
                raw_value = cells[col]
                try:
                    value = float(raw_value) / 100.0
                except Exception:
                    value = None
                row_records.append({"period": period, "code": code, "value": value})

            if row_records:
                if code not in descriptions:
                    descriptions[code] = desc
                records.extend(row_records)
                for rec in row_records:
                    periods.add(rec["period"])

    return records, descriptions, periods


def find_parent(code: str, existing: Set[str]) -> Optional[str]:
    """Find a parent code by stripping trailing characters until a known code is found."""
    if "_" not in code:
        return None
    candidate = code[:-1]
    while candidate:
        if candidate.endswith("_"):
            candidate = candidate[:-1]
            continue
        if candidate in existing:
            return candidate
        candidate = candidate[:-1]
    return None


def build_hierarchy(descriptions: Dict[str, str]) -> List[dict]:
    codes = set(descriptions.keys())
    parent_map: Dict[str, Optional[str]] = {}
    children_map: Dict[str, List[str]] = defaultdict(list)

    for code in codes:
        parent = find_parent(code, codes)
        parent_map[code] = parent
        if parent:
            children_map[parent].append(code)

    level_cache: Dict[str, int] = {}

    def level_of(code: str) -> int:
        if code in level_cache:
            return level_cache[code]
        parent = parent_map.get(code)
        if parent is None:
            level_cache[code] = 0
        else:
            level_cache[code] = level_of(parent) + 1
        return level_cache[code]

    hierarchy = []
    for code in sorted(codes):
        hierarchy.append(
            {
                "key": code,
                "label": descriptions.get(code, code),
                "parent": parent_map.get(code),
                "children": sorted(children_map.get(code, [])),
                "level": level_of(code),
            }
        )
    return hierarchy


def build_meta(
    descriptions: Dict[str, str],
    periods: Set[str],
    hierarchy: List[dict],
    generated_at: str,
) -> dict:
    sorted_periods = sorted(periods)
    first = sorted_periods[0] if sorted_periods else None
    last = sorted_periods[-1] if sorted_periods else None

    return {
        "id": "cbk_loans_interest_monthly",
        "generated_at": generated_at,
        "updated_at": f"{last}-01" if last else None,
        "time": {
            "key": "period",
            "granularity": "monthly",
            "first": first,
            "last": last,
            "count": len(sorted_periods),
        },
        "fields": [
            {
                "key": "value",
                "label": "Normat e interesit të kredive",
                "unit": "%",
                "value_type": "rate",
            }
        ],
        "metrics": ["value"],
        "dimensions": {
            "code": [
                {"key": code, "label": desc}
                for code, desc in sorted(descriptions.items())
            ]
        },
        "dimension_hierarchies": {"code": hierarchy},
        "source": "Banka Qendrore e Republikës së Kosovës – Normat e interesit për kredi",
        "source_urls": ["raw_data/loans_interest.xlsm"],
        "title": "Normat e interesit për kreditë",
        "notes": [
            "Vlerat janë norma interesi mujore; mungesat lihen bosh.",
            "Metodologjia ndryshoi në 2010; shih matricën e konvertimit në skedën '..' të burimit.",
        ],
    }


def main() -> None:
    xlsm_path = Path("raw_data/loans_interest.xlsm")
    out_path = Path("data/cbk/loan_interests.json")

    start = datetime(2010, 1, 1)
    records, descriptions, periods = load_from_excel(xlsm_path, start)
    hierarchy = build_hierarchy(descriptions)
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    meta = build_meta(descriptions, periods, hierarchy, generated_at)

    dataset = {"meta": meta, "records": sorted(records, key=lambda r: (r["period"], r["code"]))}

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(dataset, ensure_ascii=False, indent=2))
    print(f"Wrote dataset to {out_path} ({len(records)} records; {len(descriptions)} series).")


if __name__ == "__main__":
    main()
