# Energy JSON Specification

`src/fetch-entsoe.ts` emits consolidated ENTSO-E cross-border flow datasets in the `{ meta, records }` format. Energy metrics are reported in megawatt-hours (MWh) and timestamps use UTC ISO-8601 strings.

## `energy_crossborder_monthly.json`
- **Type:** Object
- **Fields:**
  - `meta` (`object`): Dataset metadata.
    - `time.granularity` (`"monthly"`), `time.first` / `time.last` (`string` periods), etc.
    - `fields` include:
      - `{ "key": "import", "label": "Importet", "unit": "MWh" }`
      - `{ "key": "export", "label": "Eksportet", "unit": "MWh" }`
      - `{ "key": "net", "label": "Bilanci neto", "unit": "MWh" }`
      - `{ "key": "has_data", "label": "Ka të dhëna", "unit": "boolean" }`
    - `dimensions.neighbor`: array of `{ key, label }` pairs for each interconnection (AL, ME, MK, RS).
  - `records` (`array`): Per-neighbor monthly rows.
    - `period` (`string`): Month in `YYYY-MM` format.
    - `neighbor` (`string`): Neighbor key (e.g., `AL`).
    - `import` (`number`): Total imports for the month.
    - `export` (`number`): Total exports for the month.
    - `net` (`number`): `import - export`.
    - `has_data` (`boolean`): Whether ENTSO-E returned data for the interconnection.

## `energy_crossborder_daily.json`
- **Type:** Object
- **Fields:**
  - `meta` (`object`): Metadata for the latest month.
    - `time.granularity` (`"daily"`), `time.first` / `time.last` (`YYYY-MM-DD`).
    - `fields` matches monthly metrics without the `neighbor` dimension.
  - `records` (`array`): Daily totals for the latest snapshot.
    - `period` (`string`): Day in `YYYY-MM-DD` format.
    - `import` (`number`)
    - `export` (`number`)
    - `net` (`number`)

### Notes
- Files are UTF-8 encoded without BOM.
- Arrays are sorted chronologically so clients can consume them directly.
- Monthly totals can be recomputed by grouping records on `period` and summing `import`, `export`, and `net`.

## Regenerating the dataset

1. Install dependencies: `pnpm install`.
2. Provide an ENTSO-E API token: `export ENTSOE_API_KEY=...`.
3. Run `pnpm run generate:energy` (optionally add `--month YYYY-MM`, `--backfill N`, or `--out <path>`). Use `--force` to re-fetch a month even if it already exists.

The generator fetches the requested months, updates `energy_crossborder_monthly.json`, and rewrites `energy_crossborder_daily.json` for the newest snapshot.
