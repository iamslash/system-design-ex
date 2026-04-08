"""Autocomplete Service.

데이터 수집(Data Gathering)과 질의(Query) 두 가지 흐름으로 구성된다.
- 데이터 수집: 검색 쿼리 로그를 집계하여 Trie 를 재구축한다.
- 질의: 접두사를 입력받아 top-k 자동완성 제안을 반환한다.
"""

from __future__ import annotations

from collections import Counter

from src.trie import Trie


class AutocompleteService:
    """검색 자동완성 서비스.

    Analytics 로그에서 쿼리 빈도를 집계하고 Trie 를 구축하여
    접두사 기반 top-k 자동완성을 제공한다.

    Args:
        k: top-k 결과 수 (기본값: 5)
        max_prefix_length: 접두사 최대 길이 (기본값: 50)
    """

    def __init__(self, k: int = 5, max_prefix_length: int = 50) -> None:
        self.trie = Trie(k=k, max_prefix_length=max_prefix_length)
        self.query_log: list[str] = []  # 시뮬레이션된 analytics 로그
        self._blocked_words: set[str] = set()  # 필터링 대상 단어

    def record_query(self, query: str) -> None:
        """검색 쿼리를 기록한다 (analytics 로깅 시뮬레이션).

        Args:
            query: 사용자가 입력한 검색 쿼리
        """
        if query:
            self.query_log.append(query.lower())

    def rebuild_trie(self) -> None:
        """쿼리 로그를 집계하여 Trie 를 재구축한다.

        주간 배치 작업을 시뮬레이션한다.
        1. 쿼리 로그에서 빈도 집계 (Aggregators)
        2. 기존 Trie 의 데이터와 병합
        3. 필터링된 단어 제외
        """
        # 1단계: 쿼리 로그 빈도 집계
        freq_counter = Counter(self.query_log)

        # 2단계: 기존 Trie 에서 단어 빈도 수집
        existing_words: dict[str, int] = {}
        self._collect_words(self.trie.root, "", existing_words)

        # 3단계: 기존 데이터와 새 데이터 병합
        merged: dict[str, int] = {}
        for word, freq in existing_words.items():
            merged[word] = freq
        for word, freq in freq_counter.items():
            merged[word] = merged.get(word, 0) + freq

        # 4단계: 필터링된 단어 제외
        for blocked in self._blocked_words:
            merged.pop(blocked.lower(), None)

        # 5단계: Trie 재구축
        self.trie = Trie(
            k=self.trie.k,
            max_prefix_length=self.trie.max_prefix_length,
        )
        self.trie.build_from_frequency_table(merged)

        # 로그 초기화 (처리 완료)
        self.query_log.clear()

    def suggest(self, prefix: str) -> list[str]:
        """접두사에 대한 top-k 자동완성 제안을 반환한다.

        필터링된 단어는 결과에서 제외된다.

        Args:
            prefix: 검색 접두사

        Returns:
            제안 단어 목록 (빈도 내림차순)
        """
        results = self.trie.search(prefix)
        # 필터링 적용
        return [
            word
            for word, _freq in results
            if word not in self._blocked_words
        ]

    def suggest_with_frequency(self, prefix: str) -> list[tuple[str, int]]:
        """접두사에 대한 top-k 자동완성 제안을 빈도와 함께 반환한다.

        Args:
            prefix: 검색 접두사

        Returns:
            [(단어, 빈도), ...] 빈도 내림차순
        """
        results = self.trie.search(prefix)
        return [
            (word, freq)
            for word, freq in results
            if word not in self._blocked_words
        ]

    def add_filter(self, blocked_words: set[str]) -> None:
        """필터 목록에 단어를 추가한다 (혐오/부적절 콘텐츠 차단).

        Args:
            blocked_words: 차단할 단어 집합
        """
        self._blocked_words.update(w.lower() for w in blocked_words)

    def remove_filter(self, words: set[str]) -> None:
        """필터 목록에서 단어를 제거한다.

        Args:
            words: 차단 해제할 단어 집합
        """
        self._blocked_words -= {w.lower() for w in words}

    @property
    def blocked_words(self) -> set[str]:
        """현재 필터링 중인 단어 집합."""
        return set(self._blocked_words)

    def _collect_words(
        self,
        node: "TrieNode",  # noqa: F821
        prefix: str,
        result: dict[str, int],
    ) -> None:
        """Trie 를 순회하며 모든 단어와 빈도를 수집한다."""
        if node.is_end:
            result[prefix] = node.frequency
        for char, child in node.children.items():
            self._collect_words(child, prefix + char, result)
