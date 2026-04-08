"""Tests for Search Autocomplete System."""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.autocomplete import AutocompleteService
from src.trie import Trie, TrieNode


# ---------------------------------------------------------------------------
# Trie 기본 테스트
# ---------------------------------------------------------------------------


class TestTrieBasic:
    """Trie 기본 동작 테스트."""

    def test_empty_trie_returns_empty(self) -> None:
        """빈 Trie 에서 검색하면 빈 리스트를 반환해야 한다."""
        trie = Trie()
        assert trie.search("abc") == []
        assert trie.word_count == 0

    def test_empty_prefix_returns_empty(self) -> None:
        """빈 접두사로 검색하면 빈 리스트를 반환해야 한다."""
        trie = Trie()
        trie.insert("hello", 10)
        assert trie.search("") == []

    def test_insert_and_search_basic(self) -> None:
        """단어를 삽입한 후 접두사로 검색할 수 있어야 한다."""
        trie = Trie()
        trie.insert("hello", 10)
        trie.insert("help", 5)

        results = trie.search("hel")
        assert len(results) == 2
        assert results[0] == ("hello", 10)
        assert results[1] == ("help", 5)

    def test_insert_single_word(self) -> None:
        """단일 단어 삽입 및 검색."""
        trie = Trie()
        trie.insert("test", 42)

        results = trie.search("test")
        assert results == [("test", 42)]

    def test_word_count_and_node_count(self) -> None:
        """word_count 와 node_count 가 정확해야 한다."""
        trie = Trie()
        assert trie.word_count == 0
        assert trie.node_count == 1  # root 노드

        trie.insert("ab", 1)
        assert trie.word_count == 1
        assert trie.node_count == 3  # root + 'a' + 'b'

        trie.insert("abc", 1)
        assert trie.word_count == 2
        assert trie.node_count == 4  # root + 'a' + 'b' + 'c'


class TestTrieTopK:
    """Top-k 캐싱 테스트."""

    def test_top_k_ordering(self) -> None:
        """빈도가 높은 순서대로 정렬되어야 한다."""
        trie = Trie(k=5)
        trie.insert("apple", 100)
        trie.insert("app", 50)
        trie.insert("application", 200)

        results = trie.search("app")
        assert results[0] == ("application", 200)
        assert results[1] == ("apple", 100)
        assert results[2] == ("app", 50)

    def test_top_k_limit(self) -> None:
        """k 개까지만 결과를 반환해야 한다."""
        trie = Trie(k=3)
        for i in range(10):
            trie.insert(f"word{i}", (i + 1) * 10)

        results = trie.search("word")
        assert len(results) == 3
        # 가장 빈도 높은 3개
        assert results[0][0] == "word9"
        assert results[1][0] == "word8"
        assert results[2][0] == "word7"

    def test_top_k_same_frequency_alphabetical(self) -> None:
        """빈도가 같을 때 알파벳 순으로 정렬해야 한다."""
        trie = Trie(k=5)
        trie.insert("cherry", 10)
        trie.insert("cat", 10)
        trie.insert("car", 10)

        results = trie.search("c")
        words = [w for w, _ in results]
        assert words == ["car", "cat", "cherry"]

    def test_prefix_matching_only(self) -> None:
        """접두사가 일치하는 단어만 반환해야 한다."""
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
        """존재하지 않는 접두사로 검색하면 빈 리스트를 반환해야 한다."""
        trie = Trie()
        trie.insert("hello", 10)
        assert trie.search("xyz") == []
        assert trie.search("z") == []


class TestTrieUpdate:
    """빈도 업데이트 테스트."""

    def test_update_frequency(self) -> None:
        """같은 단어를 다시 삽입하면 빈도가 누적되어야 한다."""
        trie = Trie()
        trie.insert("hello", 10)
        trie.insert("hello", 5)

        results = trie.search("hello")
        assert results == [("hello", 15)]

    def test_update_changes_ranking(self) -> None:
        """빈도 업데이트 후 순위가 변경되어야 한다."""
        trie = Trie()
        trie.insert("apple", 10)
        trie.insert("app", 20)

        results = trie.search("app")
        assert results[0] == ("app", 20)

        # apple 빈도를 크게 올림
        trie.insert("apple", 30)

        results = trie.search("app")
        assert results[0] == ("apple", 40)
        assert results[1] == ("app", 20)


class TestTrieDelete:
    """삭제 테스트."""

    def test_delete_word(self) -> None:
        """단어를 삭제하면 검색 결과에서 제거되어야 한다."""
        trie = Trie()
        trie.insert("hello", 10)
        trie.insert("help", 5)

        assert trie.delete("hello") is True
        results = trie.search("hel")
        assert len(results) == 1
        assert results[0] == ("help", 5)

    def test_delete_nonexistent_word(self) -> None:
        """존재하지 않는 단어 삭제는 False 를 반환해야 한다."""
        trie = Trie()
        trie.insert("hello", 10)
        assert trie.delete("world") is False

    def test_delete_empty_word(self) -> None:
        """빈 문자열 삭제는 False 를 반환해야 한다."""
        trie = Trie()
        assert trie.delete("") is False

    def test_delete_updates_word_count(self) -> None:
        """삭제 후 word_count 가 감소해야 한다."""
        trie = Trie()
        trie.insert("hello", 10)
        trie.insert("help", 5)
        assert trie.word_count == 2

        trie.delete("hello")
        assert trie.word_count == 1

    def test_delete_prefix_word_keeps_longer(self) -> None:
        """접두사인 단어를 삭제해도 더 긴 단어는 유지되어야 한다."""
        trie = Trie()
        trie.insert("app", 10)
        trie.insert("apple", 20)

        trie.delete("app")
        results = trie.search("app")
        assert len(results) == 1
        assert results[0] == ("apple", 20)


class TestTrieBuildFromTable:
    """빈도 테이블에서 구축 테스트."""

    def test_build_from_frequency_table(self) -> None:
        """빈도 테이블에서 Trie 를 구축할 수 있어야 한다."""
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
    """대소문자 무시 테스트."""

    def test_case_insensitive_insert(self) -> None:
        """대소문자를 구분하지 않고 삽입해야 한다."""
        trie = Trie()
        trie.insert("Hello", 10)
        trie.insert("HELLO", 5)

        results = trie.search("hello")
        assert results == [("hello", 15)]

    def test_case_insensitive_search(self) -> None:
        """대소문자를 구분하지 않고 검색해야 한다."""
        trie = Trie()
        trie.insert("twitter", 100)

        results = trie.search("TW")
        assert results == [("twitter", 100)]

        results = trie.search("Twitter"[:2])  # "Tw"
        assert results == [("twitter", 100)]


class TestTriePrefixLength:
    """접두사 길이 제한 테스트."""

    def test_long_prefix_truncated(self) -> None:
        """max_prefix_length 를 초과하는 접두사는 잘려야 한다."""
        trie = Trie(max_prefix_length=3)
        trie.insert("abcdef", 10)

        # "abcd" 는 3자로 잘려 "abc" 로 검색
        results = trie.search("abcdef")
        assert len(results) == 1
        assert results[0] == ("abcdef", 10)


class TestTrieUnicode:
    """유니코드/특수문자 처리 테스트."""

    def test_unicode_words(self) -> None:
        """유니코드 단어를 처리할 수 있어야 한다."""
        trie = Trie()
        trie.insert("cafe", 10)
        trie.insert("caf\u00e9", 20)

        results = trie.search("caf")
        assert len(results) == 2

    def test_special_characters(self) -> None:
        """특수 문자가 포함된 단어를 처리할 수 있어야 한다."""
        trie = Trie()
        trie.insert("c++", 50)
        trie.insert("c#", 30)

        results = trie.search("c")
        assert len(results) == 2


# ---------------------------------------------------------------------------
# AutocompleteService 테스트
# ---------------------------------------------------------------------------


class TestAutocompleteService:
    """AutocompleteService 테스트."""

    def test_suggest_basic(self) -> None:
        """기본 자동완성 제안이 동작해야 한다."""
        service = AutocompleteService(k=3)
        service.trie.build_from_frequency_table({
            "twitter": 100,
            "twitch": 80,
            "tree": 60,
        })

        suggestions = service.suggest("tw")
        assert suggestions == ["twitter", "twitch"]

    def test_suggest_returns_strings(self) -> None:
        """suggest 는 문자열 리스트를 반환해야 한다."""
        service = AutocompleteService()
        service.trie.insert("hello", 10)

        suggestions = service.suggest("hel")
        assert all(isinstance(s, str) for s in suggestions)

    def test_suggest_with_frequency(self) -> None:
        """suggest_with_frequency 는 (단어, 빈도) 튜플 리스트를 반환해야 한다."""
        service = AutocompleteService()
        service.trie.insert("hello", 10)

        results = service.suggest_with_frequency("hel")
        assert results == [("hello", 10)]


class TestAutocompleteDataGathering:
    """데이터 수집 흐름 테스트."""

    def test_record_query(self) -> None:
        """쿼리가 로그에 기록되어야 한다."""
        service = AutocompleteService()
        service.record_query("hello")
        service.record_query("world")

        assert len(service.query_log) == 2
        assert "hello" in service.query_log
        assert "world" in service.query_log

    def test_record_empty_query_ignored(self) -> None:
        """빈 쿼리는 무시되어야 한다."""
        service = AutocompleteService()
        service.record_query("")
        assert len(service.query_log) == 0

    def test_rebuild_trie_from_logs(self) -> None:
        """로그에서 Trie 를 재구축할 수 있어야 한다."""
        service = AutocompleteService(k=5)

        # 쿼리 기록
        for _ in range(5):
            service.record_query("twitter")
        for _ in range(3):
            service.record_query("twitch")
        service.record_query("tree")

        service.rebuild_trie()

        # 로그가 비워져야 함
        assert len(service.query_log) == 0

        # Trie 에 데이터가 있어야 함
        results = service.suggest_with_frequency("tw")
        assert len(results) == 2
        words = [w for w, _ in results]
        assert "twitter" in words
        assert "twitch" in words

        # twitter 빈도가 더 높아야 함
        assert results[0] == ("twitter", 5)

    def test_rebuild_merges_existing(self) -> None:
        """재구축 시 기존 데이터와 새 데이터가 병합되어야 한다."""
        service = AutocompleteService(k=5)

        # 초기 데이터
        service.trie.insert("twitter", 100)

        # 새 쿼리 기록
        for _ in range(10):
            service.record_query("twitter")

        service.rebuild_trie()

        results = service.suggest_with_frequency("tw")
        assert results[0] == ("twitter", 110)  # 100 + 10


class TestAutocompleteFilter:
    """필터링 테스트."""

    def test_filter_blocked_words(self) -> None:
        """차단된 단어가 결과에서 제외되어야 한다."""
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
        """필터가 대소문자를 구분하지 않아야 한다."""
        service = AutocompleteService()
        service.trie.insert("badword", 100)

        service.add_filter({"BadWord"})
        suggestions = service.suggest("bad")
        assert "badword" not in suggestions

    def test_remove_filter(self) -> None:
        """필터를 제거하면 다시 결과에 나타나야 한다."""
        service = AutocompleteService()
        service.trie.insert("hello", 10)

        service.add_filter({"hello"})
        assert service.suggest("hel") == []

        service.remove_filter({"hello"})
        assert service.suggest("hel") == ["hello"]

    def test_blocked_words_property(self) -> None:
        """blocked_words 프로퍼티가 현재 필터 목록을 반환해야 한다."""
        service = AutocompleteService()
        service.add_filter({"bad", "evil"})

        blocked = service.blocked_words
        assert blocked == {"bad", "evil"}

    def test_filter_applied_on_rebuild(self) -> None:
        """재구축 시 필터가 적용되어야 한다."""
        service = AutocompleteService(k=5)

        service.record_query("good")
        service.record_query("bad")
        service.add_filter({"bad"})

        service.rebuild_trie()

        # Trie 에 bad 가 없어야 함
        results = service.suggest_with_frequency("b")
        words = [w for w, _ in results]
        assert "bad" not in words


class TestAutocompleteMultipleWords:
    """여러 단어 동일 접두사 테스트."""

    def test_multiple_words_correct_ordering(self) -> None:
        """같은 접두사의 여러 단어가 빈도순으로 정렬되어야 한다."""
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
