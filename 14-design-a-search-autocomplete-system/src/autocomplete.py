"""Autocomplete Service.

Consists of two flows: Data Gathering and Query.
- Data Gathering: aggregates search query logs and rebuilds the Trie.
- Query: accepts a prefix and returns top-k autocomplete suggestions.
"""

from __future__ import annotations

from collections import Counter

from src.trie import Trie


class AutocompleteService:
    """Search autocomplete service.

    Aggregates query frequency from analytics logs, builds a Trie,
    and provides prefix-based top-k autocomplete.

    Args:
        k: Number of top-k results (default: 5)
        max_prefix_length: Maximum prefix length (default: 50)
    """

    def __init__(self, k: int = 5, max_prefix_length: int = 50) -> None:
        self.trie = Trie(k=k, max_prefix_length=max_prefix_length)
        self.query_log: list[str] = []  # simulated analytics log
        self._blocked_words: set[str] = set()  # words to filter out

    def record_query(self, query: str) -> None:
        """Record a search query (simulates analytics logging).

        Args:
            query: Search query entered by the user
        """
        if query:
            self.query_log.append(query.lower())

    def rebuild_trie(self) -> None:
        """Aggregate query logs and rebuild the Trie.

        Simulates a weekly batch job.
        1. Aggregate frequency from query logs (Aggregators)
        2. Merge with existing Trie data
        3. Exclude filtered words
        """
        # Step 1: aggregate frequency from query log
        freq_counter = Counter(self.query_log)

        # Step 2: collect word frequencies from existing Trie
        existing_words: dict[str, int] = {}
        self._collect_words(self.trie.root, "", existing_words)

        # Step 3: merge existing data with new data
        merged: dict[str, int] = {}
        for word, freq in existing_words.items():
            merged[word] = freq
        for word, freq in freq_counter.items():
            merged[word] = merged.get(word, 0) + freq

        # Step 4: remove blocked words
        for blocked in self._blocked_words:
            merged.pop(blocked.lower(), None)

        # Step 5: rebuild Trie
        self.trie = Trie(
            k=self.trie.k,
            max_prefix_length=self.trie.max_prefix_length,
        )
        self.trie.build_from_frequency_table(merged)

        # Clear log (processing complete)
        self.query_log.clear()

    def suggest(self, prefix: str) -> list[str]:
        """Return top-k autocomplete suggestions for a prefix.

        Filtered words are excluded from results.

        Args:
            prefix: Search prefix

        Returns:
            List of suggested words (descending frequency order)
        """
        results = self.trie.search(prefix)
        # Apply filter
        return [
            word
            for word, _freq in results
            if word not in self._blocked_words
        ]

    def suggest_with_frequency(self, prefix: str) -> list[tuple[str, int]]:
        """Return top-k autocomplete suggestions for a prefix with frequency.

        Args:
            prefix: Search prefix

        Returns:
            [(word, frequency), ...] in descending frequency order
        """
        results = self.trie.search(prefix)
        return [
            (word, freq)
            for word, freq in results
            if word not in self._blocked_words
        ]

    def add_filter(self, blocked_words: set[str]) -> None:
        """Add words to the filter list (block hateful/inappropriate content).

        Args:
            blocked_words: Set of words to block
        """
        self._blocked_words.update(w.lower() for w in blocked_words)

    def remove_filter(self, words: set[str]) -> None:
        """Remove words from the filter list.

        Args:
            words: Set of words to unblock
        """
        self._blocked_words -= {w.lower() for w in words}

    @property
    def blocked_words(self) -> set[str]:
        """The set of words currently being filtered."""
        return set(self._blocked_words)

    def _collect_words(
        self,
        node: "TrieNode",  # noqa: F821
        prefix: str,
        result: dict[str, int],
    ) -> None:
        """Traverse the Trie and collect all words with their frequencies."""
        if node.is_end:
            result[prefix] = node.frequency
        for char, child in node.children.items():
            self._collect_words(child, prefix + char, result)
