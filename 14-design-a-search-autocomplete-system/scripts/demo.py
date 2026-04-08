#!/usr/bin/env python3
"""Search Autocomplete Demo.

검색 자동완성 시스템의 주요 기능을 시연한다.

Run:
    python scripts/demo.py
"""

from __future__ import annotations

import os
import sys
import time

# Allow running from repo root or from the chapter directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.autocomplete import AutocompleteService
from src.trie import Trie


# 샘플 검색어 빈도 테이블
SAMPLE_FREQ_TABLE: dict[str, int] = {
    "twitter": 35000,
    "twitch": 29000,
    "twilight": 15000,
    "twin peaks": 8000,
    "twirl": 3000,
    "tree": 25000,
    "treasure": 12000,
    "trend": 22000,
    "trick": 9000,
    "try": 30000,
    "true": 28000,
    "trump": 20000,
    "trust": 18000,
    "truth": 16000,
    "toy": 14000,
    "top gun": 11000,
    "tower": 7000,
    "wish": 27000,
    "win": 24000,
    "winter": 21000,
    "wiki": 19000,
    "wild": 13000,
    "window": 10000,
    "wine": 6000,
    "apple": 40000,
    "application": 32000,
    "app store": 26000,
    "appetite": 5000,
    "approach": 17000,
    "banana": 23000,
    "band": 15000,
    "bank": 31000,
    "baseball": 20000,
    "basketball": 18000,
}


def section(title: str) -> None:
    """섹션 구분 출력."""
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)
    print()


def demo_build_trie() -> Trie:
    """1단계: 샘플 데이터로 Trie 구축."""
    section("1. Build Trie from Sample Data")

    trie = Trie(k=5)
    trie.build_from_frequency_table(SAMPLE_FREQ_TABLE)

    print(f"  Total words  : {trie.word_count}")
    print(f"  Total nodes  : {trie.node_count}")
    print(f"  Top-k (k)    : {trie.k}")
    print()

    # 몇 가지 접두사 검색 시연
    prefixes = ["tw", "tr", "wi", "ap", "ba"]
    for prefix in prefixes:
        results = trie.search(prefix)
        suggestions = ", ".join(f"{w}({f:,})" for w, f in results)
        print(f'  "{prefix}" -> {suggestions}')

    return trie


def demo_interactive(service: AutocompleteService) -> None:
    """3단계: 인터랙티브 자동완성."""
    section("3. Interactive Autocomplete")

    print("  Type a prefix to see suggestions (empty line to skip):")
    print()

    # 비대화형 환경에서는 미리 정의된 입력 사용
    demo_inputs = ["tw", "tr", "app", "ban", "wi", "to"]

    if not sys.stdin.isatty():
        inputs = demo_inputs
    else:
        print("  Demo prefixes (press Enter for each, or type your own):")
        print(f"  Suggestions: {demo_inputs}")
        print()
        inputs = []
        for default in demo_inputs:
            try:
                user_input = input(f"  prefix [{default}]: ").strip()
                inputs.append(user_input if user_input else default)
            except (EOFError, KeyboardInterrupt):
                print()
                break

    for prefix in inputs:
        results = service.suggest_with_frequency(prefix)
        if results:
            suggestions = ", ".join(f"{w}({f:,})" for w, f in results)
            print(f'  "{prefix}" -> {suggestions}')
        else:
            print(f'  "{prefix}" -> (no results)')


def demo_data_gathering(service: AutocompleteService) -> None:
    """4단계: 데이터 수집 시뮬레이션."""
    section("4. Data Gathering Simulation")

    # 새 쿼리 기록
    new_queries = [
        "twitter", "twitter", "twitter", "twitter", "twitter",
        "twitch streaming", "twitch streaming", "twitch streaming",
        "twilight zone", "twilight zone",
        "tree house", "tree house", "tree house", "tree house",
        "trend analysis",
    ]

    print(f"  Recording {len(new_queries)} new queries...")
    for q in new_queries:
        service.record_query(q)

    print(f"  Query log size: {len(service.query_log)}")
    print()

    # 기존 결과
    print('  Before rebuild - "tw" suggestions:')
    results = service.suggest_with_frequency("tw")
    for w, f in results:
        print(f"    {w}: {f:,}")
    print()

    # Trie 재구축
    print("  Rebuilding trie (simulating weekly aggregation)...")
    service.rebuild_trie()
    print(f"  Query log cleared. Size: {len(service.query_log)}")
    print()

    # 갱신된 결과
    print('  After rebuild - "tw" suggestions:')
    results = service.suggest_with_frequency("tw")
    for w, f in results:
        print(f"    {w}: {f:,}")
    print()

    print('  New entry - "tree" prefix:')
    results = service.suggest_with_frequency("tree")
    for w, f in results:
        print(f"    {w}: {f:,}")


def demo_filter(service: AutocompleteService) -> None:
    """5단계: 필터 기능 시연."""
    section("5. Filter Demonstration")

    print('  Before filter - "tr" suggestions:')
    results = service.suggest("tr")
    for w in results:
        print(f"    {w}")
    print()

    # 필터 추가
    blocked = {"trump", "trick"}
    print(f"  Adding filter: {blocked}")
    service.add_filter(blocked)
    print()

    print('  After filter - "tr" suggestions:')
    results = service.suggest("tr")
    for w in results:
        print(f"    {w}")
    print()

    print(f"  Blocked words: {service.blocked_words}")


def demo_benchmark(service: AutocompleteService) -> None:
    """6단계: 성능 벤치마크."""
    section("6. Performance Benchmark")

    prefixes = ["t", "tw", "tr", "a", "ap", "b", "ba", "w", "wi", "to"]
    num_lookups = 100_000

    print(f"  Running {num_lookups:,} prefix lookups...")
    print(f"  Prefixes: {prefixes}")
    print()

    start = time.perf_counter()
    for i in range(num_lookups):
        prefix = prefixes[i % len(prefixes)]
        service.suggest(prefix)
    elapsed = time.perf_counter() - start

    qps = num_lookups / elapsed
    avg_us = (elapsed / num_lookups) * 1_000_000

    print(f"  Total time     : {elapsed:.3f}s")
    print(f"  Lookups/sec    : {qps:,.0f}")
    print(f"  Avg per lookup : {avg_us:.1f} us")
    print()

    if qps > 100_000:
        print("  Result: Excellent - well over 100K QPS")
    elif qps > 10_000:
        print("  Result: Good - over 10K QPS")
    else:
        print("  Result: Acceptable")


def main() -> None:
    print()
    print("Search Autocomplete System Demo")
    print("================================")

    # 1. Trie 구축
    trie = demo_build_trie()

    # 2. AutocompleteService 생성
    section("2. AutocompleteService Setup")
    service = AutocompleteService(k=5)
    service.trie = Trie(k=5)
    service.trie.build_from_frequency_table(SAMPLE_FREQ_TABLE)
    print(f"  Service initialized with {service.trie.word_count} words")

    # 3. 인터랙티브 자동완성
    demo_interactive(service)

    # 4. 데이터 수집 시뮬레이션
    demo_data_gathering(service)

    # 5. 필터 시연
    demo_filter(service)

    # 6. 벤치마크
    demo_benchmark(service)

    section("Done")
    print("  All demonstrations completed successfully.")
    print()


if __name__ == "__main__":
    main()
