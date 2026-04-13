"""Trie with top-k caching at each node.

Trie implementation that caches top-k results at each node to support O(1) prefix search.
"""

from __future__ import annotations


class TrieNode:
    """An individual Trie node.

    Attributes:
        children: Map of child nodes (character -> TrieNode)
        is_end: Whether this node marks the end of a word
        frequency: Frequency count when this node is a word end
        top_k: Cached top-k results [(word, frequency), ...]
    """

    __slots__ = ("children", "is_end", "frequency", "top_k")

    def __init__(self) -> None:
        self.children: dict[str, TrieNode] = {}
        self.is_end: bool = False
        self.frequency: int = 0
        self.top_k: list[tuple[str, int]] = []


class Trie:
    """Trie with top-k caching support.

    Caches the k most frequent words starting with each prefix at each node.
    Search returns the node's top_k directly, yielding O(p) time
    where p is the prefix length.

    Args:
        k: Maximum number of results to cache at each node (default: 5)
        max_prefix_length: Maximum prefix length limit (default: 50)
    """

    def __init__(self, k: int = 5, max_prefix_length: int = 50) -> None:
        self.root = TrieNode()
        self.k = k
        self.max_prefix_length = max_prefix_length
        self._word_count = 0
        self._node_count = 1  # root node

    @property
    def word_count(self) -> int:
        """Number of words stored in the Trie."""
        return self._word_count

    @property
    def node_count(self) -> int:
        """Total number of nodes in the Trie."""
        return self._node_count

    def insert(self, word: str, frequency: int = 1) -> None:
        """Insert a word and update the top-k cache at every node along the path.

        If the word already exists, its frequency is accumulated.

        Args:
            word: Word to insert (normalized to lowercase)
            frequency: Frequency count (default: 1)
        """
        if not word:
            return

        word = word.lower()
        node = self.root

        # Also update top-k for the root node
        self._update_top_k(node, word, frequency)

        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
                self._node_count += 1
            node = node.children[char]
            # Update top-k cache at each node along the path
            self._update_top_k(node, word, frequency)

        # Mark the end-of-word node
        if not node.is_end:
            node.is_end = True
            node.frequency = frequency
            self._word_count += 1
        else:
            node.frequency += frequency

    def search(self, prefix: str) -> list[tuple[str, int]]:
        """Return top-k results for a prefix.

        Returns cached top-k in O(p) time (p = prefix length).
        Truncates prefix if it exceeds max_prefix_length.

        Args:
            prefix: Prefix to search

        Returns:
            [(word, frequency), ...] in descending frequency order, up to k results
        """
        if not prefix:
            return []

        prefix = prefix.lower()

        # Optimization 1: limit prefix length
        if len(prefix) > self.max_prefix_length:
            prefix = prefix[: self.max_prefix_length]

        node = self.root
        for char in prefix:
            if char not in node.children:
                return []
            node = node.children[char]

        # Optimization 2: return cached top-k directly
        return list(node.top_k)

    def _update_top_k(self, node: TrieNode, word: str, frequency: int) -> None:
        """Update the top-k cache at a node.

        If the same word already exists, accumulate frequency; otherwise add it,
        then sort by descending frequency and keep only the top k.

        Args:
            node: Node to update
            word: Word
            frequency: Frequency to add
        """
        # Check existing entries
        for i, (w, f) in enumerate(node.top_k):
            if w == word:
                node.top_k[i] = (w, f + frequency)
                # Sort by descending frequency
                node.top_k.sort(key=lambda x: (-x[1], x[0]))
                return

        # Add new entry
        node.top_k.append((word, frequency))
        node.top_k.sort(key=lambda x: (-x[1], x[0]))

        # Trim if exceeding k
        if len(node.top_k) > self.k:
            node.top_k = node.top_k[: self.k]

    def build_from_frequency_table(self, freq_table: dict[str, int]) -> None:
        """Build the Trie from a frequency table.

        Args:
            freq_table: {word: frequency} dictionary
        """
        for word, freq in freq_table.items():
            self.insert(word, freq)

    def delete(self, word: str) -> bool:
        """Delete a word and remove it from the top-k cache along the path.

        Args:
            word: Word to delete

        Returns:
            True if deletion succeeded, False otherwise
        """
        if not word:
            return False

        word = word.lower()

        # Collect nodes along the path
        path: list[TrieNode] = [self.root]
        node = self.root
        for char in word:
            if char not in node.children:
                return False
            node = node.children[char]
            path.append(node)

        # Nothing to delete if not a word end
        if not node.is_end:
            return False

        # Clear the end-of-word flag
        node.is_end = False
        node.frequency = 0
        self._word_count -= 1

        # Remove the word from top-k cache at every node along the path
        for path_node in path:
            path_node.top_k = [
                (w, f) for w, f in path_node.top_k if w != word
            ]

        # Clean up unnecessary nodes (reverse order from leaf)
        for i in range(len(word) - 1, -1, -1):
            char = word[i]
            parent = path[i]
            child = path[i + 1]

            if child.children or child.is_end:
                break
            del parent.children[char]
            self._node_count -= 1

        return True
