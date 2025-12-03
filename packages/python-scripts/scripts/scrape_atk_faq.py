#!/usr/bin/env python
"""
Scrape all question/answer pairs from https://www.atk-ks.org/pyetje-te-shpeshta/.

Outputs a JSON file (array of FAQs) with question, answer HTML, and a short
hashed id derived from the element anchor on the page.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import os
import hashlib
import ssl
import sys
import time
from dataclasses import dataclass
from typing import Iterable, List, Set, Tuple
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

BASE_URL = "https://www.atk-ks.org/pyetje-te-shpeshta/"
USER_AGENT = "Mozilla/5.0 (compatible; atk-faq-scraper/1.0; +https://kosovatools.org)"
SSL_CONTEXT = ssl._create_unverified_context()
PHONE_PATTERN = re.compile(r"(?<!\d)(04[3459](?:[\s\-/]?\d){6})(?!\d)")
EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
HASH_PREFIX = "faq-"
HASH_DIGEST_SIZE = 8  # Produces 16 hex chars; we trim further below.
HASH_LENGTH = 12
HASHED_ID_PATTERN = re.compile(rf"^{HASH_PREFIX}[0-9a-f]{{{HASH_LENGTH}}}$")


def faq_key(question: str, faq_id: str | None) -> str:
    """Stable key for de-duplication: prefer hashed id, otherwise question text."""
    return faq_id or question


def mask_question(text: str) -> str:
    """Redact local phone numbers and email addresses in question text."""
    if not text:
        return text
    masked = PHONE_PATTERN.sub("[PHONE]", text)
    masked = EMAIL_PATTERN.sub("[EMAIL]", masked)
    return masked


def normalize_id(source_id: str | None, question: str) -> str:
    """
    Build a short, stable id from the source anchor (preferred) or question text.

    - Existing hashed ids are passed through untouched.
    - Uses blake2s for speed; trimmed to HASH_LENGTH hex characters with a prefix.
    """
    candidate = (source_id or "").strip()
    if HASHED_ID_PATTERN.match(candidate):
        return candidate

    base = candidate or question or "atk-faq"
    digest = hashlib.blake2s(base.encode("utf-8"), digest_size=HASH_DIGEST_SIZE).hexdigest()
    return f"{HASH_PREFIX}{digest[:HASH_LENGTH]}"


@dataclass
class FAQ:
    page: int
    question: str
    answer_html: str
    id: str | None


def answer_text(answer_html: str) -> str:
    """Extract plain text from an answer block."""
    if not answer_html:
        return ""
    return BeautifulSoup(answer_html, "lxml").get_text(" ", strip=True)


def is_placeholder_answer(answer_html: str) -> bool:
    """Detect empty placeholder responses on the site."""
    return answer_text(answer_html).lower() == "please fill in an answer"


def clean_answer_html(answer_html: str) -> str:
    """
    Strip placeholder paragraphs and normalize simple HTML into plain text with newlines.
    """
    if not answer_html:
        return ""
    soup = BeautifulSoup(answer_html, "lxml")
    for p in soup.find_all("p"):
        if p.get_text(" ", strip=True).lower() == "please fill in an answer":
            p.decompose()
    for br in soup.find_all("br"):
        br.replace_with("\n")

    body = soup.body or soup
    blocks: List[str] = []
    for tag in body.find_all(["p", "li", "div"], recursive=False):
        text = tag.get_text("\n", strip=True)
        if text:
            blocks.append(text)

    if not blocks:
        text = body.get_text("\n", strip=True)
        return text.strip()

    normalized = "\n\n".join(blocks)
    # Collapse trailing spaces on each line and trim.
    normalized = "\n".join(line.rstrip() for line in normalized.splitlines())
    return normalized.strip()


def pick_best_faq(current: FAQ, candidate: FAQ) -> FAQ:
    """
    Choose the better FAQ entry when encountering duplicates.

    Preference order:
    1. Non-placeholder answers over placeholders.
    2. Longer answer HTML wins if both are placeholders or both are real answers.
    """
    current_placeholder = is_placeholder_answer(current.answer_html)
    candidate_placeholder = is_placeholder_answer(candidate.answer_html)

    if current_placeholder and not candidate_placeholder:
        return candidate
    if candidate_placeholder and not current_placeholder:
        return current

    if len(candidate.answer_html.strip()) > len(current.answer_html.strip()):
        return candidate
    return current


def dedupe_faqs(faqs: Iterable[FAQ]) -> List[FAQ]:
    """
    Deduplicate FAQs by their stable key, keeping the best answer for each key.
    """
    ordered_keys: List[str] = []
    best: dict[str, FAQ] = {}

    for faq in faqs:
        key = faq_key(faq.question, faq.id)
        if key in best:
            best[key] = pick_best_faq(best[key], faq)
        else:
            ordered_keys.append(key)
            best[key] = faq

    return [best[key] for key in ordered_keys]


def fetch_html(page: int) -> bytes:
    url = BASE_URL if page == 1 else f"{BASE_URL}?wpfaqpage={page}"
    req = Request(url, headers={"User-Agent": USER_AGENT})
    print(f"Fetching page {page}...", file=sys.stderr)
    with urlopen(req, context=SSL_CONTEXT) as resp:
        return resp.read()


def parse_faqs(soup: BeautifulSoup, page: int) -> List[FAQ]:
    faqs: List[FAQ] = []
    for holder in soup.select(".wpfaq-question-holder"):
        question_el = holder.select_one("h4.wpfaqacctoggle")
        raw_question = question_el.get_text(" ", strip=True) if question_el else ""
        question = mask_question(raw_question)

        source_id = None
        anchor_el = holder.select_one("h4.wpfaqacctoggle a[href]")
        if anchor_el and anchor_el.has_attr("href"):
            href = anchor_el["href"]
            source_id = href.split("#", 1)[-1] if "#" in href else href

        content = holder.select_one(".wpfaqacccontent .wpfaqacccontenti")
        if not content:
            content = holder.select_one(".wpfaqacccontent")

        answer_html = clean_answer_html(content.decode_contents().strip()) if content else ""
        hashed_id = normalize_id(source_id, question)

        faqs.append(
            FAQ(
                page=page,
                question=question,
                answer_html=answer_html,
                id=hashed_id,
            )
        )
    return faqs


def extract_total(soup: BeautifulSoup) -> int | None:
    marker = soup.select_one(".faqs-paging .displaying-num")
    if not marker:
        return None
    match = re.search(r"of\s+(\d+)", marker.get_text())
    return int(match.group(1)) if match else None


def scrape_all(
    start_page: int = 1,
    pages: int | None = None,
    delay: float = 0.1,
    seen: Set[str] | None = None,
) -> List[FAQ]:
    """
    Scrape pages and return FAQs, skipping items whose key is in seen.
    """
    seen = seen or set()

    first_html = fetch_html(1)
    first_soup = BeautifulSoup(first_html, "lxml")
    total = extract_total(first_soup)  # May be None if paging not present.
    first_page_faqs = parse_faqs(first_soup, 1)

    per_page = len(first_page_faqs) or 1
    # If caller passed a number of pages, treat it as a window size starting at start_page.
    total_pages = start_page + pages - 1 if pages else (math.ceil(total / per_page) if total else 1)

    all_faqs: List[FAQ] = []
    if start_page == 1:
        all_faqs.extend(first_page_faqs)

    for page in range(max(start_page, 2), total_pages + 1):
        html = fetch_html(page)
        soup = BeautifulSoup(html, "lxml")
        faqs = parse_faqs(soup, page)
        all_faqs.extend(faqs)
        if delay:
            time.sleep(delay)
    # Filter out any seen (question, id) combos.
    if seen:
        all_faqs = [f for f in all_faqs if faq_key(f.question, f.id) not in seen]
    return all_faqs


def save_json(faqs: Iterable[FAQ], path: str) -> None:
    deduped = dedupe_faqs(faqs)
    deduped = [faq for faq in deduped if answer_text(faq.answer_html)]
    payload = [
        {
            "question": mask_question(faq.question),
            "answer_html": faq.answer_html,
            "id": faq.id,
        }
        for faq in deduped
    ]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_existing(path: str) -> Tuple[List[FAQ], Set[str]]:
    faqs: List[FAQ] = []
    if not os.path.exists(path):
        return faqs, set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for item in data:
                raw_question = item.get("question", "")
                question = mask_question(raw_question)
                source_id = item.get("id") or item.get("anchor")
                faq_id = normalize_id(source_id, question)
                answer_html = clean_answer_html(item.get("answer_html", ""))
                faqs.append(
                    FAQ(
                        page=0,
                        question=question,
                        answer_html=answer_html,
                        id=faq_id,
                    )
                )
    except Exception:
        pass

    deduped = dedupe_faqs(faqs)
    seen: Set[str] = {faq_key(faq.question, faq.id) for faq in deduped}
    return deduped, seen


def load_state(path: str) -> int:
    if not os.path.exists(path):
        return 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return int(data.get("last_page", 0))
    except Exception:
        return 0


def save_state(path: str, last_page: int) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"last_page": last_page}, f)
    except Exception:
        pass


def dump_streaming(
    out_path: str,
    start_page: int,
    pages: int | None,
    delay: float,
    resume: bool,
    state_file: str,
    max_empty_pages: int,
) -> int:
    existing, seen_existing = load_existing(out_path) if resume else ([], set())
    seen: Set[str] = set(seen_existing)
    last_page_state = load_state(state_file) if resume else 0

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    if state_file:
        os.makedirs(os.path.dirname(state_file) or ".", exist_ok=True)

    print(f"Start page: {start_page}", file=sys.stderr)
    if resume and seen:
        print(
            f"Existing data detected ({len(seen)} items). Starting from page {start_page} and stopping after {max_empty_pages} empty page(s).",
            file=sys.stderr,
        )
    if last_page_state:
        print(f"Last recorded page in state: {last_page_state}", file=sys.stderr)

    # Always fetch first page to learn totals/per-page count.
    first_html = fetch_html(1)
    first_soup = BeautifulSoup(first_html, "lxml")
    total = extract_total(first_soup)
    first_page_faqs = parse_faqs(first_soup, 1)

    per_page = len(first_page_faqs) or 5
    computed_total_pages = math.ceil(total / per_page) if total else 1
    total_pages = start_page + pages - 1 if pages else computed_total_pages

    added = 0
    empty_streak = 0
    for page in range(start_page, total_pages + 1):
        if page == 1:
            soup = first_soup
        else:
            html = fetch_html(page)
            soup = BeautifulSoup(html, "lxml")

        faqs = parse_faqs(soup, page)
        new_faqs = [f for f in faqs if faq_key(f.question, f.id) not in seen]
        print(
            f"Page {page}: found {len(faqs)} items, new {len(new_faqs)}",
            file=sys.stderr,
        )
        stop_after_save = False
        if not new_faqs:
            empty_streak += 1
            if empty_streak >= max_empty_pages:
                stop_after_save = True
        else:
            empty_streak = 0

        for faq in new_faqs:
            existing.append(faq)
            added += 1
            seen.add(faq_key(faq.question, faq.id))

        save_json(existing, out_path)
        save_state(state_file, page)
        if delay and page < total_pages:
            time.sleep(delay)
        if stop_after_save:
            print(
                f"Stopping after {empty_streak} consecutive pages with no new items.",
                file=sys.stderr,
            )
            break

    # Ensure file is saved even if no new items were added.
    save_json(existing, out_path)
    return added


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape ATK FAQ entries.")
    parser.add_argument("--output", "-o", default="data/atk/atk_faq.json", help="Output JSON path.")
    parser.add_argument("--start-page", type=int, default=1, help="Page number to start from.")
    parser.add_argument("--pages", type=int, default=None, help="Number of pages to fetch (window size).")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Delay between page requests in seconds.",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Start fresh (ignore existing output/state and overwrite output).",
    )
    parser.add_argument(
        "--state-file",
        default="raw_data/atk_faq.state",
        help="Path to store scraping state (last page).",
    )
    parser.add_argument(
        "--max-empty-pages",
        type=int,
        default=1,
        help="Stop after this many consecutive pages with no new items (use 1 to halt on first empty page).",
    )
    args = parser.parse_args()

    print("Scraping ATK FAQ…", file=sys.stderr)
    added = dump_streaming(
        out_path=args.output,
        start_page=max(1, args.start_page),
        pages=args.pages,
        delay=args.delay,
        resume=not args.fresh,
        state_file=args.state_file or f"{args.output}.state",
        max_empty_pages=max(1, args.max_empty_pages),
    )
    print(f"Added {added} entries to {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
