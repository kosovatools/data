# Kosovo Data Tools

This repo is a pnpm workspace that holds data pipelines (TS and Python) plus generated snapshots for Kosovo datasets (energy, turnover, etc.).

## Workspace layout
- `packages/entsoe`: ENTSO-E importer (`pnpm run generate:energy`)
- `packages/customs`: Customs tariff fetcher/cleaner (`pnpm run generate:customs`)
- `packages/kas-pxweb`: ASKdata PxWeb fetchers (`pnpm run generate:kas`)
- `packages/data-types`: shared dataset typings (`@kosovatools/data-types`)
- `packages/python-scripts`: Python utilities (turnover, ATK FAQ, loan interests, etc.)

Run `pnpm install` in this directory to bootstrap the workspace.

## Dataset hygiene
- Dimension keys must be slug-safe: lowercase ASCII, underscores instead of spaces/diacritics, stable across reruns. Always slugify human-readable labels before writing JSON (e.g., a `slugify` helper that strips accents and collapses whitespace/punctuation).
- If you introduce new dimensions, dedupe slugs to avoid collisions (append suffixes as needed) and keep the original labels in metadata.
- Generated files should stay UTF-8 without BOM and keep `meta`/`records` structures consistent across refreshes.
- Shared dataset typings for the platform live under `packages/data-types/types` (packaged as `@kosovatools/data-types` for the site to consume via a file dependency).

## Turnover data
Run `pnpm run python:turnover -- --source raw_data --output data/mfk/turnover` after placing the latest `turnover-<year>.xlsx` files in `raw_data/`. You can also call the script directly via `venv/bin/python packages/python-scripts/scripts/generate_turnover_json.py ...`.

## KAS PxWeb data
- Run `pnpm run generate:kas` to fetch ASKdata PxWeb tables via the `packages/kas-pxweb` pipeline. Outputs land in `data/kas/*.json` using the standard `{ meta, records }` shape.

## Customs data
Run `pnpm run generate:customs` with `CUSTOMS_DATA_SOURCE_URL` in the environment. The cleaned dataset is written to `data/customs/tarrifs.json`.

## Energy data
Run `pnpm run generate:energy -- --out ./data/energy` with `ENTSOE_API_KEY` in the environment. Neighbor codes are slugged in the dataset; keep any new dimension keys slug-safe.
