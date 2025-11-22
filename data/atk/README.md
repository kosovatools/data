# ATK FAQ Dataset

This directory contains scraped Frequently Asked Questions from the Kosovo Tax Administration (ATK) page at `https://www.atk-ks.org/pyetje-te-shpeshta/`. New questions appear on the first page of the ATK site, so the scraper starts from page 1 each run and stops when it encounters a page with no new items.

## `atk_faq.json`
- **Type:** Array of objects
- **Fields per entry:**
  - `id` (`string|null`): Short, stable id derived from the page anchor (blake2s hash, e.g. `faq-abc123def456`).
  - `question` (`string`): Question text.
  - `answer_html` (`string`): Answer text normalized to plain text; paragraphs become blank-line-separated, and `<br>` become `\n`.
- **Ordering:** As scraped from the site (page 1 first). File is UTF-8 encoded.
- Phone numbers in questions matching `04[3/4/5/9]` followed by six digits (spaces, dashes, or slashes allowed) are masked as `[PHONE]`. Email addresses are masked as `[EMAIL]`.

## Regeneration

```bash
python scripts/scrape_atk_faq.py \
  --output data/atk/atk_faq.json \
  --state-file raw_data/atk_faq.state \
  --max-empty-pages 1
```

- By default the script deduplicates against existing entries, writes JSON and halts after the first page with zero new items. This matches the site’s behavior where new FAQs appear on page 1.
- Use `--fresh` to rebuild from scratch (ignores existing data/state). Use `--pages N` to fetch only the first N pages for a quick check.
- The state file is a small JSON blob containing the last page visited for logging/visibility. It is not used to skip pages; the scraper always starts from page 1 and stops when it reaches only previously-seen items.
