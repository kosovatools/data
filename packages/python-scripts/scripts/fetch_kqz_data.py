#!/usr/bin/env python3
import argparse
from pathlib import Path
from urllib.request import Request, urlopen


URLS = [
    "https://storage.kqz-ks.org/metadata-national.json",
    "https://storage.kqz-ks.org/full-candidates-parliamentary.json",
    "https://storage.kqz-ks.org/qnr-metadata-parliamentary.json",
    "https://storage.kqz-ks.org/total-metadata-parliamentary.json",
    "https://storage.kqz-ks.org/parliamentary-qkn-latest.json",
    "https://storage.kqz-ks.org/parliamentary-qnr-latest.json",
]


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)


def download(url: str, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request) as response:
        content = response.read()
    dest.write_bytes(content)


def main():
    default_dir = Path(__file__).resolve().parents[3] / "data" / "kqz"
    parser = argparse.ArgumentParser(description="Fetch KQZ datasets into data folder.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_dir,
        help="Destination directory for downloaded JSON files",
    )
    args = parser.parse_args()

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    for url in URLS:
        filename = url.split("/")[-1]
        destination = output_dir / filename
        download(url, destination)
        print(f"Fetched {url} -> {destination}")


if __name__ == "__main__":
    main()
