# Prishtina Building Permits

Official building permit registries published by Komuna e Prishtinës (Drejtoria e Urbanizmit) for 2012‑2026. The 2026 registry is the latest published partial-year workbook. The Excel workbooks live in `raw_data/prishtina/building-permits-<year>.xlsx` and are parsed by `packages/python-scripts/scripts/generate_prishtina_building_permits.py` into JSON exports under `data/prishtina/building_permits/`.

## Raw downloads

| Year | Workbook URL |
| --- | --- |
| 2026 | https://www.prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2026%20(5).xlsx |
| 2025 | https://www.prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2025%20(8).xlsx |
| 2024 | https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2024%20%2810%29.xlsx |
| 2023 | https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2023%20%281%29.xlsx |
| 2022 | https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_2022_23.03.2023.xlsx |
| 2021 | https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2021_13.12.2021.xlsx |
| 2020 | https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2020_final%20%281%29.xlsx |
| 2019 | https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2019%20%2811%29.xlsx |
| 2018 | https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2018%20%283%29.xlsx |
| 2017 | https://prishtinaonline.com/uploads/lista-e-lejeve-te-leshuara-per-vitin-2017%20%286%29.xlsx |
| 2016 | https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2016%20%2816%29.xlsx |
| 2015 | https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2015%20%288%29.xlsx |
| 2014 | https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2014.xlsx |
| 2013 | https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2013.xlsx |
| 2012 | https://prishtinaonline.com/uploads/lista_e_lejeve_te_leshuara_per_vitin_2012.xlsx |

## Outputs

### `permits_<year>.json`
One file per workbook (e.g., `permits_2024.json`) with:

- `year`, `generated_at`, `source_url`, `record_count`.
- `records`: permit rows in workbook order. Fields:
  - `permit_number` (`string`)
  - `application_date`, `issuance_date` (`string | null`, ISO `YYYY-MM-DD`)
  - `application_dates` (optional `string[]` of ISO dates, present when the workbook lists multiple application dates; `application_date` stores the first)
  - `owner`, `investor`, `designer`, `neighbourhood` (`string | null`)
  - `total_floor_area_m2`, `density_fee_eur`, `administrative_fee_eur`, `total_fee_eur` (`number | null`)
  - `storeys`, `destination`, `document_reference` (`string | null`)
  - `document_url`, `situation_url` (`string | null`) — extracted from the Excel hyperlinks pointing to the PDF permit and the situational map.

### Documented sites and planned ground floors

`total_floor_area_m2` is the total construction floor area reported in the municipal register. Two optional fields add measurements from identifiable pages of the linked documents.

- `site.parcels` lists cadastral parcel references and their stated land areas in square metres where documented. The source URL and one-based PDF page are stored in `site`.
- Each element of `footprints` describes one planned ground-floor outline. It includes the permit PDF URL, one-based page, WGS84 GeoJSON polygon and horizontal area in square metres calculated from the survey vertices. A permit can cover several buildings.

The reviewed transcriptions in [prishtina-permit-sites.json](../../../packages/python-scripts/sources/prishtina-permit-sites.json) are keyed by the permit PDF URL. Survey vertices follow the outline in the municipal diagram, in KOSOVAREF01 easting and northing (metres). The generator calculates area in that coordinate system and converts the vertices from EPSG:9141 to longitude and latitude. Only verified complete ground-floor rings are exported. Coverage varies by year and permit. [Kosovo Cadastral Agency survey guide](https://akk.rks-gov.net/storage/app/media/udhezuesi-per-matje-krijim-ndryshim-dhe-regjistrim-te-njesive-kadastrale-parcele-ndertese-dhe-pjese-te-nderteses.pdf)

### `index.json`

Summary metadata used by downstream tooling:

- `generated_at`: UTC timestamp of the latest index generation. Unchanged per-year files retain their original timestamps.
- `years`: array describing each workbook with `year`, `records_file`, and `record_count`. The entries are ordered from the latest year to the oldest, and `records_file` is stored relative to this directory (e.g., `permits_2025.json`). Use the per-year files themselves for the source URL.

## Regeneration

```bash
uv run --script packages/python-scripts/scripts/generate_prishtina_building_permits.py \
  --raw-dir raw_data/prishtina \
  --output data/prishtina/building_permits
```

Drop new workbooks into `raw_data/prishtina/` (following the `building-permits-<year>.xlsx` naming pattern) before running the script.

To download fresh copies of the 2025–2026 workbooks and update only those years, use an empty raw directory and a matching filename pattern:

```bash
uv run --script packages/python-scripts/scripts/generate_prishtina_building_permits.py \
  --download-source \
  --raw-dir "$(mktemp -d)" \
  --pattern 'building-permits-202[56].xlsx' \
  --output data/prishtina/building_permits
```

The index is rebuilt from all per-year JSON files in the output directory, including unchanged historical exports.

## Structure notes

| Years | Row count range | Columns |
| --- | --- | --- |
| 2012‑2015 | 83‑158 rows | `#`, `Data e lëshimit të lejes`, owner, investor, designer, neighborhood, total floor area, total fee, floors, comment (used to infer destination), PDF link, situational plan. |
| 2016‑2017 | 100‑161 rows | Adds `Data e aplikimit të lejes`, detailed density fee (13.30€/6.70€), administrative fee, `Destinimi i objektit`. |
| 2018‑2026 | 86‑177 rows | Same columns as 2016‑2017 but density fee updated to dual tariff (4.30€/10.70€) and admin fee rounded to 6.50€/m². Column text occasionally truncates the trailing `m²`, but the content is consistent. PDF and `Situacioni` (map) links are included when provided. |

Additional observations:

- Header text is preceded by banner rows that must be skipped; the actual header is the first row containing `Data e lëshimit të lejes`.
- Permit numbering restarts every year (`#` column).
- The totals in the Excel sheet are already denominated in Euros; no conversion is performed by the parser.
- Explicit fee exemptions (“Lirohet nga pagesa” or “Lirohet nga taksa”) are exported as zero rather than missing amounts.
- Dates are transcribed as published, including dates outside the workbook's nominal year.
- Hyperlinks are embedded inside Excel cells (display text such as “Leja dokumenti…” or “Situacioni”); the parser reads the hyperlink target to surface the actual URL.
- The workbooks embed Albanian-only text and UTF‑8 characters; all JSON exports stay in UTF‑8.
- Older (2012‑2015) sheets label `Destinimi` as a free-form “Koment”; the generator copies that value into `destination` when needed and omits a separate `comment` field from the JSON.
