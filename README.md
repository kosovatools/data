# Kosovo Data Tools

This repo holds scripts and generated snapshots for various Kosovo datasets (energy, turnover, etc.).

## Dataset hygiene
- Dimension keys must be slug-safe: lowercase ASCII, underscores instead of spaces/diacritics, stable across reruns. Always slugify human-readable labels before writing JSON (e.g., a `slugify` helper that strips accents and collapses whitespace/punctuation).
- If you introduce new dimensions, dedupe slugs to avoid collisions (append suffixes as needed) and keep the original labels in metadata.
- Generated files should stay UTF-8 without BOM and keep `meta`/`records` structures consistent across refreshes.
- Shared dataset typings for the platform live under `src/kas/types` (packaged as `@kosovatools/data-types` for the site to consume via a file dependency).

## Turnover data
Run `venv/bin/python scripts/generate_turnover_json.py --source raw_data --output data/mfk/turnover` after placing the latest `turnover-<year>.xlsx` files in `raw_data/`.

## KAS PxWeb data
- Run `npm run generate:kas` (from this `data/` directory) to fetch ASKdata PxWeb tables via the `src/kas/scripts` pipeline. Outputs land in `data/kas/*.json` using the standard `{ meta, records }` shape.

## Energy data
Run `npm run generate:energy -- --out ./data/energy` with `ENTSOE_API_KEY` in the environment. Neighbor codes are slugged in the dataset; keep any new dimension keys slug-safe.
