"""Tests for Search Autocomplete System."""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.autocomplete import AutocompleteService
from src.trie import Trie, TrieNode


# ---------------------------------------------------------------------------
# Trie basic tests
# ---------------------------------------------------------------------------


class TestTrieBasic:
    """Basic Trie behavior tests."""

    def test_empty_trie_returns_empty(self) -> None:
        """Searching an empty Trie should return an empty list."""
        trie = Trie()
        assert trie.search("abc") == []
        assert trie.word_count == 0

    def test_empty_prefix_returns_empty(self) -> None:
        """Searching with an empty prefix should return an empty list."""
        trie = Trie()
        trie.insert("hello", 10)
        assert trie.search("") == []

    def test_insert_and_search_basic(self) -> None:
        """After inserting words, they should be searchable by prefix."""
        trie = Trie()
        trie.insert("hello", 10)
        trie.insert("help", 5)

        results = trie.search("hel")
        assert len(results) == 2
        assert results[0] == ("hello", 10)
        assert results[1] == ("help", 5)

    def test_insert_single_word(self) -> None:
        """Insert and search a single word."""
        trie = Trie()
        trie.insert("test", 42)

        results = trie.search("test")
        assert results == [("test", 42)]

    def test_word_count_and_node_count(self) -> None:
        """word_count and node_count should be accurate."""
        trie = Trie()
        assert trie.word_count == 0
        assert trie.node_count == 1  # root node

        trie.insert("ab", 1)
        assert trie.word_count == 1
        assert trie.node_count == 3  # root + 'a' + 'b'

        trie.insert("abc", 1)
        assert trie.word_count == 2
        assert trie.node_count == 4  # root + 'a' + 'b' + 'c'


class TestTrieTopK:
    """Top-k caching tests."""

    def test_top_k_ordering(self) -> None:
        """Results should be sorted in descending frequency order."""
        trie = Trie(k=5)
        trie.insert("apple", 100)
        trie.insert("app", 50)
        trie.insert("application", 200)

        results = trie.search("app")
        assert results[0] == ("application", 200)
        assert results[1] == ("apple", 100)
        assert results[2] == ("app", 50)

    def test_top_k_limit(self) -> None:
        """Should return at most k results."""
        trie = Trie(k=3)
        for i in range(10):
            trie.insert(f"word{i}", (i + 1) * 10)

        results = trie.search("word")
        assert len(results) == 3
        # Top 3 by frequency
        assert results[0][0] == "word9"
        assert results[1][0] == "word8"
        assert results[2][0] == "word7"

    def test_top_k_same_frequency_alphabetical(self) -> None:
        """When frequencies are equal, results should be sorted alphabetically."""
        trie = Trie(k=5)
        trie.insert("cherry", 10)
        trie.insert("cat", 10)
        trie.insert("car", 10)

        results = trie.search("c")
        words = [w for w, _ in results]
        assert words == ["car", "cat", "cherry"]

    def test_prefix_matching_only(self) -> None:
        """Only words matching the prefix should be returned."""
        trie = Trie()
        trie.insert("twitter", 100)
        trie.insert("twitch", 80)
        trie.insert("tree", 60)

        results = trie.search("tw")
        words = [w for w, _ in results]
        assert "twitter" in words
        assert "twitch" in words
        assert "tree" not in words

    def test_no_results_for_nonexistent_prefix(self) -> None:
        """Searching with a non-existent prefix should return an empty list."""
        trie = Trie()
        trie.insert("hello", 10)
        assert trie.search("xyz") == []
        assert trie.search("z") == []


class TestTrieUpdate:
    """Frequency update tests."""

    def test_update_frequency(self) -> None:
        """Re-inserting the same word should accumulate frequency."""
        trie = Trie()
        trie.insert("hello", 10)
        trie.insert("hello", 5)

        results = trie.search("hello")
        assert results == [("hello", 15)]

    def test_update_changes_ranking(self) -> None:
        """Ranking should change after a frequency update."""
        trie = Trie()
        trie.insert("apple", 10)
        trie.insert("app", 20)

        results = trie.search("app")
        assert results[0] == ("app", 20)

        # Boost apple frequency significantly
        trie.insert("apple", 30)

        results = trie.search("app")
        assert results[0] == ("apple", 40)
        assert results[1] == ("app", 20)


class TestTrieDelete:
    """Deletion tests."""

    def test_delete_word(self) -> None:
        """Deleting a word should remove it from search results."""
        trie = Trie()
        trie.insert("hello", 10)
        trie.insert("help", 5)

        assert trie.delete("hello") is True
        results = trie.search("hel")
        assert len(results) == 1
        assert results[0] == ("help", 5)

    def test_delete_nonexistent_word(self) -> None:
        """Deleting a non-existent word should return False."""
        trie = Trie()
        trie.insert("hello", 10)
        assert trie.delete("world") is False

    def test_delete_empty_word(self) -> None:
        """Deleting an empty string should return False."""
        trie = Trie()
        assert trie.delete("") is False

    def test_delete_updates_word_count(self) -> None:
        """word_count should decrease after deletion."""
        trie = Trie()
        trie.insert("hello", 10)
        trie.insert("help", 5)
        assert trie.word_count == 2

        trie.delete("hello")
        assert trie.word_count == 1

    def test_delete_prefix_word_keeps_longer(self) -> None:
        """Deleting a prefix word should keep longer words intact."""
        trie = Trie()
        trie.insert("app", 10)
        trie.insert("apple", 20)

        trie.delete("app")
        results = trie.search("app")
        assert len(results) == 1
        assert results[0] == ("apple", 20)


class TestTrieBuildFromTable:
    """Build-from-frequency-table tests."""

    def test_build_from_frequency_table(self) -> None:
        """Should be able to build a Trie from a frequency table."""
        trie = Trie(k=3)
        freq_table = {
            "twitter": 100,
            "twitch": 80,
            "twilight": 60,
            "tree": 50,
            "try": 40,
        }
        trie.build_from_frequency_table(freq_table)

        assert trie.word_count == 5

        results = trie.search("tw")
        assert len(results) == 3
        assert results[0] == ("twitter", 100)
        assert results[1] == ("twitch", 80)
        assert results[2] == ("twilight", 60)


class TestTrieCaseInsensitive:
    """Case-insensitive tests."""

    def test_case_insensitive_insert(self) -> None:
        """Should insert words case-insensitively."""
        trie = Trie()
        trie.insert("Hello", 10)
        trie.insert("HELLO", 5)

        results = trie.search("hello")
        assert results == [("hello", 15)]

    def test_case_insensitive_search(self) -> None:
        """Should search case-insensitively."""
        trie = Trie()
        trie.insert("twitter", 100)

        results = trie.search("TW")
        assert results == [("twitter", 100)]

        results = trie.search("Twitter"[:2])  # "Tw"
        assert results == [("twitter", 100)]


class TestTriePrefixLength:
    """Prefix length limit tests."""

    def test_long_prefix_truncated(self) -> None:
        """Prefixes exceeding max_prefix_length should be truncated."""
        trie = Trie(max_prefix_length=3)
        trie.insert("abcdef", 10)

        # "abcd" is truncated to 3 chars and searched as "abc"
        results = trie.search("abcdef")
        assert len(results) == 1
        assert results[0] == ("abcdef", 10)


class TestTrieUnicode:
    """Unicode and special character handling tests."""

    def test_unicode_words(self) -> None:
        """Should be able to handle unicode words."""
        trie = Trie()
        trie.insert("cafe", 10)
        trie.insert("caf\u00e9", 20)

        results = trie.search("caf")
        assert len(results) == 2

    def test_special_characters(self) -> None:
        """Should be able to handle words containing special characters."""
        trie = Trie()
        trie.insert("c++", 50)
        trie.insert("c#", 30)

        results = trie.search("c")
        assert len(results) == 2


# ---------------------------------------------------------------------------
# AutocompleteService tests
# ---------------------------------------------------------------------------


class TestAutocompleteService:
    """AutocompleteService tests."""

    def test_suggest_basic(self) -> None:
        """Basic autocomplete suggestions should work."""
        service = AutocompleteService(k=3)
        service.trie.build_from_frequency_table({
            "twitter": 100,
            "twitch": 80,
            "tree": 60,
        })

        suggestions = service.suggest("tw")
        assert suggestions == ["twitter", "twitch"]

    def test_suggest_returns_strings(self) -> None:
        """suggest should return a list of strings."""
        service = AutocompleteService()
        service.trie.insert("hello", 10)

        suggestions = service.suggest("hel")
        assert all(isinstance(s, str) for s in suggestions)

    def test_suggest_with_frequency(self) -> None:
        """suggest_with_frequency should return a list of (word, frequency) tuples."""
        service = AutocompleteService()
        service.trie.insert("hello", 10)

        results = service.suggest_with_frequency("hel")
        assert results == [("hello", 10)]


class TestAutocompleteDataGathering:
    """Data gathering flow tests."""

    def test_record_query(self) -> None:
        """Queries should be recorded in the log."""
        service = AutocompleteService()
        service.record_query("hello")
        service.record_query("world")

        assert len(service.query_log) == 2
        assert "hello" in service.query_log
        assert "world" in service.query_log

    def test_record_empty_query_ignored(self) -> None:
        """Empty queries should be ignored."""
        service = AutocompleteService()
        service.record_query("")
        assert len(service.query_log) == 0

    def test_rebuild_trie_from_logs(self) -> None:
        """Should be able to rebuild the Trie from logs."""
        service = AutocompleteService(k=5)

        # Record queries
        for _ in range(5):
            service.record_query("twitter")
        for _ in range(3):
            service.record_query("twitch")
        service.record_query("tree")

        service.rebuild_trie()

        # Log should be cleared
        assert len(service.query_log) == 0

        # Trie should contain data
        results = service.suggest_with_frequency("tw")
        assert len(results) == 2
        words = [w for w, _ in results]
        assert "twitter" in words
        assert "twitch" in words

        # twitter should have higher frequency
        assert results[0] == ("twitter", 5)

    def test_rebuild_merges_existing(self) -> None:
        """Existing data and new data should be merged on rebuild."""
        service = AutocompleteService(k=5)

        # Initial data
        service.trie.insert("twitter", 100)

        # Record new queries
        for _ in range(10):
            service.record_query("twitter")

        service.rebuild_trie()

        results = service.suggest_with_frequency("tw")
        assert results[0] == ("twitter", 110)  # 100 + 10


class TestAutocompleteFilter:
    """Filtering tests."""

    def test_filter_blocked_words(self) -> None:
        """Blocked words should be excluded from results."""
        service = AutocompleteService()
        service.trie.build_from_frequency_table({
            "twitter": 100,
            "toxic": 80,
            "tree": 60,
        })

        service.add_filter({"toxic"})

        suggestions = service.suggest("t")
        assert "toxic" not in suggestions
        assert "twitter" in suggestions
        assert "tree" in suggestions

    def test_filter_case_insensitive(self) -> None:
        """The filter should be case-insensitive."""
        service = AutocompleteService()
        service.trie.insert("badword", 100)

        service.add_filter({"BadWord"})
        suggestions = service.suggest("bad")
        assert "badword" not in suggestions

    def test_remove_filter(self) -> None:
        """Removing a filter should allow the word to appear in results again."""
        service = AutocompleteService()
        service.trie.insert("hello", 10)

        service.add_filter({"hello"})
        assert service.suggest("hel") == []

        service.remove_filter({"hello"})
        assert service.suggest("hel") == ["hello"]

    def test_blocked_words_property(self) -> None:
        """The blocked_words property should return the current filter list."""
        service = AutocompleteService()
        service.add_filter({"bad", "evil"})

        blocked = service.blocked_words
        assert blocked == {"bad", "evil"}

    def test_filter_applied_on_rebuild(self) -> None:
        """Filters should be applied during rebuild."""
        service = AutocompleteService(k=5)

        service.record_query("good")
        service.record_query("bad")
        service.add_filter({"bad"})

        service.rebuild_trie()

        # 'bad' should not be in the Trie
        results = service.suggest_with_frequency("b")
        words = [w for w, _ in results]
        assert "bad" not in words


class TestAutocompleteMultipleWords:
    """Multiple words with same prefix tests."""

    def test_multiple_words_correct_ordering(self) -> None:
        """Multiple words with the same prefix should be sorted by frequency."""
        service = AutocompleteService(k=5)
        freq_table = {
            "twitter": 35000,
            "twitch": 29000,
            "twilight": 15000,
            "twin peaks": 8000,
            "twirl": 3000,
        }
        service.trie.build_from_frequency_table(freq_table)

        results = service.suggest_with_frequency("tw")
        assert len(results) == 5
        assert results[0] == ("twitter", 35000)
        assert results[1] == ("twitch", 29000)
        assert results[2] == ("twilight", 15000)
        assert results[3] == ("twin peaks", 8000)
        assert results[4] == ("twirl", 3000)
