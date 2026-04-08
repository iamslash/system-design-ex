"""Trie with top-k caching at each node.

각 노드에 top-k 결과를 캐싱하여 O(1) 접두사 검색을 지원하는 Trie 구현.
"""

from __future__ import annotations


class TrieNode:
    """Trie 의 개별 노드.

    Attributes:
        children: 자식 노드 맵 (문자 -> TrieNode)
        is_end: 단어의 끝인지 여부
        frequency: 이 노드가 단어 끝일 때의 빈도수
        top_k: 캐싱된 top-k 결과 [(단어, 빈도), ...]
    """

    __slots__ = ("children", "is_end", "frequency", "top_k")

    def __init__(self) -> None:
        self.children: dict[str, TrieNode] = {}
        self.is_end: bool = False
        self.frequency: int = 0
        self.top_k: list[tuple[str, int]] = []


class Trie:
    """Top-k 캐싱을 지원하는 Trie.

    각 노드에 해당 접두사로 시작하는 단어 중 빈도가 가장 높은 k 개를
    캐싱한다. 검색 시 노드의 top_k 를 바로 반환하므로 O(p) 시간에
    결과를 얻을 수 있다 (p = 접두사 길이).

    Args:
        k: 각 노드에 캐싱할 최대 결과 수 (기본값: 5)
        max_prefix_length: 접두사 최대 길이 제한 (기본값: 50)
    """

    def __init__(self, k: int = 5, max_prefix_length: int = 50) -> None:
        self.root = TrieNode()
        self.k = k
        self.max_prefix_length = max_prefix_length
        self._word_count = 0
        self._node_count = 1  # root 노드

    @property
    def word_count(self) -> int:
        """Trie 에 저장된 단어 수."""
        return self._word_count

    @property
    def node_count(self) -> int:
        """Trie 의 전체 노드 수."""
        return self._node_count

    def insert(self, word: str, frequency: int = 1) -> None:
        """단어를 삽입하고 경로의 모든 노드에서 top-k 캐시를 갱신한다.

        이미 존재하는 단어인 경우 빈도를 누적한다.

        Args:
            word: 삽입할 단어 (소문자로 정규화됨)
            frequency: 빈도수 (기본값: 1)
        """
        if not word:
            return

        word = word.lower()
        node = self.root

        # 루트 노드의 top-k 도 갱신
        self._update_top_k(node, word, frequency)

        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
                self._node_count += 1
            node = node.children[char]
            # 경로의 각 노드에서 top-k 캐시 갱신
            self._update_top_k(node, word, frequency)

        # 단어 끝 노드 설정
        if not node.is_end:
            node.is_end = True
            node.frequency = frequency
            self._word_count += 1
        else:
            node.frequency += frequency

    def search(self, prefix: str) -> list[tuple[str, int]]:
        """접두사에 대한 top-k 결과를 반환한다.

        캐싱된 top-k 를 반환하므로 O(p) 시간 (p = 접두사 길이).
        접두사 길이가 max_prefix_length 를 초과하면 잘라낸다.

        Args:
            prefix: 검색할 접두사

        Returns:
            [(단어, 빈도), ...] 빈도 내림차순, 최대 k 개
        """
        if not prefix:
            return []

        prefix = prefix.lower()

        # 최적화 1: 접두사 길이 제한
        if len(prefix) > self.max_prefix_length:
            prefix = prefix[: self.max_prefix_length]

        node = self.root
        for char in prefix:
            if char not in node.children:
                return []
            node = node.children[char]

        # 최적화 2: 캐싱된 top-k 바로 반환
        return list(node.top_k)

    def _update_top_k(self, node: TrieNode, word: str, frequency: int) -> None:
        """노드의 top-k 캐시를 갱신한다.

        기존에 같은 단어가 있으면 빈도를 누적하고, 없으면 추가한 뒤
        빈도 내림차순으로 정렬하여 상위 k 개만 유지한다.

        Args:
            node: 갱신할 노드
            word: 단어
            frequency: 추가할 빈도수
        """
        # 기존 항목 확인
        for i, (w, f) in enumerate(node.top_k):
            if w == word:
                node.top_k[i] = (w, f + frequency)
                # 빈도 내림차순 정렬
                node.top_k.sort(key=lambda x: (-x[1], x[0]))
                return

        # 새 항목 추가
        node.top_k.append((word, frequency))
        node.top_k.sort(key=lambda x: (-x[1], x[0]))

        # k 개 초과 시 잘라내기
        if len(node.top_k) > self.k:
            node.top_k = node.top_k[: self.k]

    def build_from_frequency_table(self, freq_table: dict[str, int]) -> None:
        """빈도 테이블에서 Trie 를 구축한다.

        Args:
            freq_table: {단어: 빈도수} 딕셔너리
        """
        for word, freq in freq_table.items():
            self.insert(word, freq)

    def delete(self, word: str) -> bool:
        """단어를 삭제하고 경로의 top-k 캐시에서도 제거한다.

        Args:
            word: 삭제할 단어

        Returns:
            삭제 성공 여부
        """
        if not word:
            return False

        word = word.lower()

        # 경로의 노드들을 수집
        path: list[TrieNode] = [self.root]
        node = self.root
        for char in word:
            if char not in node.children:
                return False
            node = node.children[char]
            path.append(node)

        # 단어 끝이 아니면 삭제할 것이 없음
        if not node.is_end:
            return False

        # 단어 끝 플래그 해제
        node.is_end = False
        node.frequency = 0
        self._word_count -= 1

        # 경로의 모든 노드에서 top-k 캐시에서 해당 단어 제거
        for path_node in path:
            path_node.top_k = [
                (w, f) for w, f in path_node.top_k if w != word
            ]

        # 불필요한 노드 정리 (리프 노드부터 역순으로)
        for i in range(len(word) - 1, -1, -1):
            char = word[i]
            parent = path[i]
            child = path[i + 1]

            if child.children or child.is_end:
                break
            del parent.children[char]
            self._node_count -= 1

        return True
