#!/usr/bin/env python3
"""Web Crawler Demo.

Demonstrates BFS web crawling with URL frontier, dedup, and robots.txt.

Run:
    python scripts/demo.py
    python scripts/demo.py --seed https://quotes.toscrape.com/ --max-pages 10
"""

from __future__ import annotations

import argparse
import os
import sys
import time

# Allow running from repo root or from the chapter directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.crawler import CrawlResult, WebCrawler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BFS Web Crawler Demo")
    parser.add_argument(
        "--seed",
        type=str,
        default="https://quotes.toscrape.com/",
        help="Seed URL to start crawling (default: https://quotes.toscrape.com/)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=20,
        help="Maximum number of pages to crawl (default: 20)",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=2,
        help="Maximum crawl depth (default: 2)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Politeness delay in seconds (default: 0.5)",
    )
    return parser.parse_args()


def print_result(idx: int, result: CrawlResult) -> None:
    """Print a single crawl result on one line."""
    status = f"{result.status_code}" if result.status_code else "ERR"
    dup = " [DUP]" if result.content_duplicate else ""
    err = f" ({result.error})" if result.error else ""
    title = f'  "{result.title}"' if result.title else ""

    print(
        f"  [{idx:3d}] {status:>3s}  depth={result.depth}  "
        f"links={result.links_found:<3d}{dup}{err}{title}"
    )
    print(f"        {result.url}")


def main() -> None:
    args = parse_args()
    seed_url = args.seed

    print()
    print("Web Crawler Demo")
    print("================")
    print()
    print(f"  Seed URL       : {seed_url}")
    print(f"  Max Pages      : {args.max_pages}")
    print(f"  Max Depth      : {args.max_depth}")
    print(f"  Politeness Delay: {args.delay}s")
    print()

    print("=" * 70)
    print("  Crawling...")
    print("=" * 70)
    print()

    crawler = WebCrawler(
        seed_urls=[seed_url],
        max_pages=args.max_pages,
        max_depth=args.max_depth,
        politeness_delay=args.delay,
        request_timeout=10.0,
    )

    try:
        results, stats = crawler.crawl()
    except KeyboardInterrupt:
        print("\n  Crawl interrupted by user.")
        return
    except Exception as e:
        print(f"\n  Crawl failed: {e}")
        print()
        print("  The target site may be unavailable. Try:")
        print("    python scripts/demo.py --seed https://books.toscrape.com/")
        print("    python scripts/demo.py --seed https://httpbin.org/")
        print()
        return

    # Print results
    for idx, result in enumerate(results, 1):
        print_result(idx, result)

    print()
    print("=" * 70)
    print("  Summary")
    print("=" * 70)
    print()
    print(f"  Pages crawled      : {stats.pages_crawled}")
    print(f"  Pages failed       : {stats.pages_failed}")
    print(f"  Content duplicates : {stats.content_duplicates}")
    print(f"  Robots.txt blocked : {stats.robots_blocked}")
    print(f"  URLs discovered    : {stats.urls_discovered}")
    print(f"  Elapsed time       : {stats.elapsed_seconds:.2f}s")

    if stats.pages_crawled > 0:
        avg = stats.elapsed_seconds / stats.pages_crawled
        print(f"  Avg time per page  : {avg:.2f}s")

    print()

    # List of crawled URLs
    successful = [r for r in results if r.status_code == 200 and not r.error]
    if successful:
        print("=" * 70)
        print("  Successfully Crawled URLs")
        print("=" * 70)
        print()
        for r in successful:
            dup_mark = " [DUP]" if r.content_duplicate else ""
            print(f"    depth={r.depth}  {r.url}{dup_mark}")
        print()

    print("Done.")
    print()


if __name__ == "__main__":
    main()
