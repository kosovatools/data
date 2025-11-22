# CBK Loan Interest Dataset

Monthly interest rate series from the Central Bank of Kosovo (CBK) extracted directly from `raw_data/loans_interest.xlsm` (sheet `IntRates_Loans`), serialized in the common `{ meta, records }` format.

## `loan_interests.json`
- **Type:** Object with `meta` and `records`.
- **Meta highlights:**
  - `id`: `cbk_loans_interest_monthly`
  - `time`: monthly periods (`period` key), with `first`, `last`, and `count`. Data starts at `2010-01` for consistency post-methodology change.
  - `fields`: single metric `{ "key": "value", "label": "Normat e interesit te kredive", "unit": "%", "value_type": "rate" }`, stored as decimal rates (values divided by 100 from the workbook).
  - `metrics`: `["value"]`.
  - `dimensions.code`: array of `{ key, label }` for each of the 302 series codes from the workbook.
  - `dimension_hierarchies.code`: parent/child tree derived from code prefixes (e.g., `T` -> `T_4` -> `T_4Ac`).
  - `source`: CBK interest rate publication; `source_urls`: `raw_data/loans_interest.xlsm`.
  - `notes`: include the 2010 methodology change and that missing values remain blank/null.
- **Records (array):**
  - `period` (`string`): `YYYY-MM`.
  - `code` (`string`): series code from the workbook.
  - `value` (`number|null`): monthly rate in decimal form (e.g., `0.051` for 5.1%); `null` where the sheet is blank or non-numeric.
- Files are UTF-8, sorted by `period`, then `code`.

## Regeneration

Ensure `raw_data/loans_interest.xlsm` is present (updated workbook). Then run:

```bash
./venv/bin/python scripts/export_loans_interest_dataset.py
```

The script reads the `IntRates_Loans` sheet directly from the XLSM and writes `data/cbk/loan_interests.json`, rebuilding `meta` (including hierarchies) and rewriting `records`.***
