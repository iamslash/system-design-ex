# Design A Search Autocomplete System

검색 자동완성(Search Autocomplete)은 사용자가 검색창에 글자를 입력할 때마다
가장 인기 있는 검색어 k 개를 실시간으로 제안하는 시스템이다.
예를 들어 `tw` 를 입력하면 `twitter`, `twitch`, `twilight` 등이 제안된다.

## 아키텍처

```
┌────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│ Analytics  │────▶│  Aggregators │────▶│   Trie DB   │────▶│  Trie Cache  │
│   Logs     │     │  (주간 집계)  │     │  (영구 저장) │     │  (인메모리)   │
└────────────┘     └──────────────┘     └─────────────┘     └──────┬───────┘
                                                                   │
┌────────────┐     ┌──────────────┐                                │
│   Client   │────▶│  API Server  │────────────────────────────────┘
│  (Browser) │◀────│  (Query)     │  top-k 결과 반환 (캐시 hit → O(1))
└────────────┘     └──────────────┘
```

### 두 가지 흐름

| 흐름 | 역할 | 주기 |
|------|------|------|
| **Data Gathering** | 검색 로그 수집 → 빈도 집계 → Trie 재구축 | 주간 배치 |
| **Query Service** | 접두사 수신 → Trie 캐시에서 top-k 반환 | 실시간 |

## 개략적 규모 추정 (Back-of-the-Envelope Estimation)

| 항목 | 수치 |
|------|------|
| 일일 활성 사용자 (DAU) | 10M (1,000만) |
| 1인당 일일 검색 횟수 | 10회 |
| 검색 당 평균 타이핑 문자 수 | 20자 |
| 검색 당 자동완성 요청 수 | ~20회 (매 키 입력마다) |
| 일일 자동완성 QPS | 10M × 10 × 20 / 86,400 ≈ **~24,000 QPS** |
| 피크 QPS (× 2) | ~48,000 QPS |
| 검색어 평균 길이 | 20 bytes |
| 일일 새 검색 데이터 | 10M × 10 × 20 bytes ≈ **~0.4 GB/day** |
| 연간 새 검색 데이터 | ~146 GB/year |

## 해시 값 길이 계산

자동완성과 직접 관련은 없지만, 검색어를 해시로 저장하거나 샤딩 키로
활용할 때의 참고 수치:

| 길이 (n) | 가능한 조합 (62^n) | 용도 |
|----------|--------------------|------|
| 4 | 14.8M (1,480만) | 소규모 캐시 키 |
| 5 | 916M (9.16억) | 중규모 |
| **6** | **56.8B (568억)** | **대부분의 검색어 커버** |
| 7 | 3.5T (3.5조) | URL 단축 등 |

## Trie 자료구조

Trie (접두사 트리)는 문자열의 공통 접두사를 공유하는 트리 구조다.
각 노드에 **top-k 결과를 캐싱**하여 검색 시 O(p) 시간에 결과를 반환한다
(p = 접두사 길이).

```
                    (root)
                   /  |   \
                 a    t    w
                /    / \    \
              p    r   w    i
             / \   |   |    |
            p   p  e   i    s
           /    |  |   |    |
          l     r  e   t    h
          |     o     / \
          e    a    t   c
               c    e   h
               h    r
```

### 노드 구조

각 노드는 자식 맵, 단어 끝 플래그, 빈도, 그리고 **캐싱된 top-k 결과**를 갖는다.

```python
class TrieNode:
    """Trie 의 개별 노드."""

    __slots__ = ("children", "is_end", "frequency", "top_k")

    def __init__(self) -> None:
        self.children: dict[str, TrieNode] = {}  # 자식 노드 맵
        self.is_end: bool = False                 # 단어의 끝인지 여부
        self.frequency: int = 0                   # 빈도수
        self.top_k: list[tuple[str, int]] = []    # 캐싱된 top-k [(단어, 빈도), ...]
```

### 두 가지 최적화

#### 최적화 1: 접두사 길이 제한

사용자가 50자 이상의 접두사를 입력하는 경우는 극히 드물다.
`max_prefix_length` 를 설정하여 긴 접두사를 잘라내면 불필요한 탐색을 방지한다.

```python
# 검색 시 접두사 길이 제한
if len(prefix) > self.max_prefix_length:
    prefix = prefix[: self.max_prefix_length]
```

#### 최적화 2: 각 노드에 Top-k 캐싱

전통적인 Trie 에서는 접두사 노드를 찾은 뒤 하위 트리를 전부 탐색해야
top-k 를 구할 수 있다 (O(n)). **각 노드에 top-k 를 캐싱**하면
노드를 찾는 즉시 결과를 반환할 수 있다 (O(1) 추가 비용).

```
노드 "tw" 의 top_k 캐시:
  [("twitter", 35000), ("twitch", 29000), ("twilight", 15000),
   ("twin peaks", 8000), ("twirl", 3000)]

→ "tw" 검색 시 이 캐시를 바로 반환 (하위 트리 순회 불필요)
```

## 핵심 구현

### 1. Trie 클래스

```python
class Trie:
    """Top-k 캐싱을 지원하는 Trie.

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

    def insert(self, word: str, frequency: int = 1) -> None:
        """단어를 삽입하고 경로의 모든 노드에서 top-k 캐시를 갱신한다."""
        if not word:
            return

        word = word.lower()
        node = self.root
        self._update_top_k(node, word, frequency)  # 루트 노드도 갱신

        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
                self._node_count += 1
            node = node.children[char]
            self._update_top_k(node, word, frequency)  # 경로의 각 노드 갱신

        if not node.is_end:
            node.is_end = True
            node.frequency = frequency
            self._word_count += 1
        else:
            node.frequency += frequency  # 기존 단어면 빈도 누적

    def search(self, prefix: str) -> list[tuple[str, int]]:
        """접두사에 대한 top-k 결과를 반환한다. O(p) 시간."""
        if not prefix:
            return []

        prefix = prefix.lower()
        if len(prefix) > self.max_prefix_length:
            prefix = prefix[: self.max_prefix_length]

        node = self.root
        for char in prefix:
            if char not in node.children:
                return []
            node = node.children[char]

        return list(node.top_k)  # 캐싱된 결과 바로 반환

    def _update_top_k(self, node: TrieNode, word: str, frequency: int) -> None:
        """노드의 top-k 캐시를 갱신한다.
        기존 단어면 빈도 누적, 없으면 추가 후 정렬하여 k 개 유지.
        """
        for i, (w, f) in enumerate(node.top_k):
            if w == word:
                node.top_k[i] = (w, f + frequency)
                node.top_k.sort(key=lambda x: (-x[1], x[0]))
                return

        node.top_k.append((word, frequency))
        node.top_k.sort(key=lambda x: (-x[1], x[0]))  # 빈도 내림차순, 동률 시 알파벳순

        if len(node.top_k) > self.k:
            node.top_k = node.top_k[: self.k]

    def build_from_frequency_table(self, freq_table: dict[str, int]) -> None:
        """빈도 테이블에서 Trie 를 구축한다."""
        for word, freq in freq_table.items():
            self.insert(word, freq)

    def delete(self, word: str) -> bool:
        """단어를 삭제하고 경로의 top-k 캐시에서도 제거한다."""
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

        if not node.is_end:
            return False

        node.is_end = False
        node.frequency = 0
        self._word_count -= 1

        # 경로의 모든 노드에서 top-k 캐시 갱신
        for path_node in path:
            path_node.top_k = [(w, f) for w, f in path_node.top_k if w != word]

        # 불필요한 리프 노드 정리
        for i in range(len(word) - 1, -1, -1):
            parent = path[i]
            child = path[i + 1]
            if child.children or child.is_end:
                break
            del parent.children[word[i]]
            self._node_count -= 1

        return True
```

### 2. Autocomplete Service

```python
class AutocompleteService:
    """검색 자동완성 서비스.

    Analytics 로그에서 쿼리 빈도를 집계하고 Trie 를 구축하여
    접두사 기반 top-k 자동완성을 제공한다.
    """

    def __init__(self, k: int = 5, max_prefix_length: int = 50) -> None:
        self.trie = Trie(k=k, max_prefix_length=max_prefix_length)
        self.query_log: list[str] = []       # 시뮬레이션된 analytics 로그
        self._blocked_words: set[str] = set()  # 필터링 대상 단어

    def record_query(self, query: str) -> None:
        """검색 쿼리를 기록한다 (analytics 로깅 시뮬레이션)."""
        if query:
            self.query_log.append(query.lower())

    def rebuild_trie(self) -> None:
        """쿼리 로그를 집계하여 Trie 를 재구축한다 (주간 배치 시뮬레이션).
        1. 쿼리 로그 빈도 집계 (Aggregators)
        2. 기존 Trie 데이터와 병합
        3. 필터링된 단어 제외
        4. Trie 재구축
        """
        freq_counter = Counter(self.query_log)

        # 기존 데이터 수집 및 병합
        existing: dict[str, int] = {}
        self._collect_words(self.trie.root, "", existing)

        merged: dict[str, int] = {}
        for word, freq in existing.items():
            merged[word] = freq
        for word, freq in freq_counter.items():
            merged[word] = merged.get(word, 0) + freq

        # 필터 적용
        for blocked in self._blocked_words:
            merged.pop(blocked.lower(), None)

        # 재구축
        self.trie = Trie(k=self.trie.k, max_prefix_length=self.trie.max_prefix_length)
        self.trie.build_from_frequency_table(merged)
        self.query_log.clear()

    def suggest(self, prefix: str) -> list[str]:
        """접두사에 대한 top-k 자동완성 제안을 반환한다."""
        results = self.trie.search(prefix)
        return [word for word, _freq in results if word not in self._blocked_words]

    def add_filter(self, blocked_words: set[str]) -> None:
        """필터 목록에 단어를 추가한다 (혐오/부적절 콘텐츠 차단)."""
        self._blocked_words.update(w.lower() for w in blocked_words)
```

## Data Gathering Service 흐름

검색 로그를 수집하고 주기적으로 Trie 를 재구축하는 비실시간 파이프라인이다.

```
┌────────────┐     ┌──────────────┐     ┌─────────────────┐     ┌──────────────┐
│  Search    │     │  Analytics   │     │   Aggregators   │     │   Trie DB    │
│  Queries   │     │    Logs      │     │                 │     │              │
└─────┬──────┘     └──────┬───────┘     └────────┬────────┘     └──────┬───────┘
      │                   │                      │                     │
      │  1. 사용자 검색    │                      │                     │
      │─────────────────▶│                      │                     │
      │                   │                      │                     │
      │                   │  2. 주간 배치 시작     │                     │
      │                   │─────────────────────▶│                     │
      │                   │                      │                     │
      │                   │                      │  3. 빈도 집계         │
      │                   │                      │  Counter(logs)      │
      │                   │                      │                     │
      │                   │                      │  4. 기존 데이터 병합   │
      │                   │                      │  existing + new     │
      │                   │                      │                     │
      │                   │                      │  5. Trie 재구축      │
      │                   │                      │─────────────────────▶│
      │                   │                      │                     │
      │                   │                      │  6. 캐시 배포         │
      │                   │                      │─────────────────────▶│ Trie Cache
```

### 집계 예시

```
로그: ["twitter", "twitter", "twitch", "twitter", "twitch", "tree"]

집계 결과:
  twitter: 3
  twitch:  2
  tree:    1

기존 Trie 데이터와 병합:
  twitter: 35000 + 3 = 35003
  twitch:  29000 + 2 = 29002
  tree:    25000 + 1 = 25001
```

## Query Service 흐름

사용자의 키 입력마다 Trie 캐시에서 top-k 결과를 반환한다.

```
Client                          API Server                 Trie Cache
  │                                │                          │
  │  "t" 입력                       │                          │
  │  GET /autocomplete?q=t         │                          │
  │───────────────────────────────▶│                          │
  │                                │  trie.search("t")        │
  │                                │─────────────────────────▶│
  │                                │◀─────────────────────────│
  │  ["twitter","try","true",      │  top-k 캐시 hit (O(1))   │
  │   "twitch","tree"]             │                          │
  │◀───────────────────────────────│                          │
  │                                │                          │
  │  "tw" 입력                      │                          │
  │  GET /autocomplete?q=tw        │                          │
  │───────────────────────────────▶│                          │
  │                                │  trie.search("tw")       │
  │                                │─────────────────────────▶│
  │                                │◀─────────────────────────│
  │  ["twitter","twitch",          │                          │
  │   "twilight","twin peaks",     │                          │
  │   "twirl"]                     │                          │
  │◀───────────────────────────────│                          │
```

## Trie 연산

### Create (구축)

빈도 테이블에서 Trie 를 구축한다. 각 단어를 삽입하면서 경로의 모든 노드에
top-k 캐시를 갱신한다.

```python
freq_table = {"twitter": 35000, "twitch": 29000, "twilight": 15000}
trie = Trie(k=5)
trie.build_from_frequency_table(freq_table)
```

### Update (갱신)

같은 단어를 다시 삽입하면 빈도가 누적된다. 주간 배치에서 새 로그를
집계하여 기존 빈도에 더한다.

```python
# 기존: twitter=35000
trie.insert("twitter", 5)
# 결과: twitter=35005 (빈도 누적)
```

### Delete (삭제) + Filter Layer

삭제는 두 가지 계층에서 수행된다:

1. **Trie 삭제**: 단어를 Trie 에서 직접 제거 (경로 노드의 top-k 도 갱신)
2. **필터 계층**: 혐오/부적절 콘텐츠를 차단 목록에 추가 (Trie 구조는 유지)

```python
# 직접 삭제
trie.delete("badword")

# 필터 계층 (검색 결과에서만 제외)
service.add_filter({"inappropriate", "offensive"})
```

필터 계층은 Trie 재구축 없이 실시간으로 적용할 수 있어 운영상 유리하다.

## 저장소 확장: 샤딩

검색어가 수억 개로 늘어나면 단일 서버의 메모리로 감당할 수 없다.
**첫 글자 기준 샤딩**으로 Trie 를 분산한다.

```
┌──────────────────────────────────────────────┐
│               Shard Router                    │
│  prefix[0] → shard 결정                       │
└──────────────┬───────────────┬───────────────┘
               │               │
    ┌──────────▼──┐  ┌────────▼───────┐  ┌──────────────┐
    │  Shard A-E  │  │  Shard F-J     │  │  Shard K-O   │  ...
    │  (a,b,c,d,e)│  │  (f,g,h,i,j)  │  │  (k,l,m,n,o) │
    └─────────────┘  └────────────────┘  └──────────────┘
```

| 샤드 | 담당 문자 | 비고 |
|------|----------|------|
| Shard 1 | a - e | 고빈도 문자 |
| Shard 2 | f - j | |
| Shard 3 | k - o | |
| Shard 4 | p - t | 고빈도 문자 (t 포함) |
| Shard 5 | u - z | 저빈도 문자 통합 가능 |

실제로는 검색어 분포를 분석하여 빈도가 높은 문자에 더 많은 샤드를
할당한다 (예: `t`, `s` 는 단독 샤드).

## Quick Start

```bash
cd 14-design-a-search-autocomplete-system

# 의존성 설치 (pytest 만 필요)
pip install -r requirements.txt

# 테스트 실행
python -m pytest tests/ -v

# 데모 실행
python scripts/demo.py
```

## 데모 사용법

```bash
python scripts/demo.py
```

### 데모 출력 예시

```
Search Autocomplete System Demo
================================

======================================================================
  1. Build Trie from Sample Data
======================================================================

  Total words  : 34
  Total nodes  : 121
  Top-k (k)    : 5

  "tw" -> twitter(35,000), twitch(29,000), twilight(15,000), twin peaks(8,000), twirl(3,000)
  "tr" -> try(30,000), true(28,000), tree(25,000), trend(22,000), trump(20,000)
  "wi" -> wish(27,000), win(24,000), winter(21,000), wiki(19,000), wild(13,000)
  "ap" -> apple(40,000), application(32,000), app store(26,000), approach(17,000), appetite(5,000)
  "ba" -> bank(31,000), banana(23,000), baseball(20,000), basketball(18,000), band(15,000)

======================================================================
  4. Data Gathering Simulation
======================================================================

  Recording 15 new queries...

  Before rebuild - "tw" suggestions:
    twitter: 35,000
    twitch: 29,000

  Rebuilding trie (simulating weekly aggregation)...

  After rebuild - "tw" suggestions:
    twitter: 35,005    ← 빈도 갱신됨
    twitch: 29,000

  New entry - "tree" prefix:
    tree: 25,000
    tree house: 4      ← 새 검색어 추가됨

======================================================================
  5. Filter Demonstration
======================================================================

  Before filter - "tr" suggestions:
    try, true, tree, trend, trump

  Adding filter: {'trick', 'trump'}

  After filter - "tr" suggestions:
    try, true, tree, trend         ← trump 제외됨

======================================================================
  6. Performance Benchmark
======================================================================

  Running 100,000 prefix lookups...

  Total time     : 0.037s
  Lookups/sec    : 2,736,278
  Avg per lookup : 0.4 us

  Result: Excellent - well over 100K QPS
```

## 테스트 실행

```bash
python -m pytest tests/ -v
```

### 테스트 항목

| 테스트 | 설명 |
|--------|------|
| Empty trie returns empty | 빈 Trie 검색 시 빈 리스트 반환 |
| Empty prefix returns empty | 빈 접두사 검색 시 빈 리스트 반환 |
| Insert and search basic | 삽입 후 기본 접두사 검색 |
| Insert single word | 단일 단어 삽입 및 검색 |
| Word/node count | 단어 수, 노드 수 정확성 |
| Top-k ordering | 빈도 내림차순 정렬 |
| Top-k limit | k 개까지만 결과 반환 |
| Same frequency alphabetical | 동일 빈도 시 알파벳순 |
| Prefix matching only | 접두사 일치 단어만 반환 |
| No results for nonexistent | 없는 접두사 → 빈 결과 |
| Update frequency | 중복 삽입 시 빈도 누적 |
| Update changes ranking | 빈도 갱신 후 순위 변경 |
| Delete word | 단어 삭제 후 결과에서 제거 |
| Delete nonexistent | 없는 단어 삭제 시 False |
| Delete empty word | 빈 문자열 삭제 시 False |
| Delete updates count | 삭제 후 word_count 감소 |
| Delete prefix keeps longer | 접두사 삭제 시 긴 단어 유지 |
| Build from frequency table | 빈도 테이블에서 일괄 구축 |
| Case insensitive insert | 대소문자 무시 삽입 |
| Case insensitive search | 대소문자 무시 검색 |
| Long prefix truncated | 긴 접두사 자동 절삭 |
| Unicode words | 유니코드 단어 처리 |
| Special characters | 특수 문자 (c++, c#) 처리 |
| Suggest basic | 기본 자동완성 제안 |
| Suggest returns strings | 문자열 리스트 반환 확인 |
| Suggest with frequency | 빈도 포함 제안 |
| Record query | 쿼리 로그 기록 |
| Record empty ignored | 빈 쿼리 무시 |
| Rebuild from logs | 로그에서 Trie 재구축 |
| Rebuild merges existing | 기존 데이터와 병합 |
| Filter blocked words | 차단 단어 결과 제외 |
| Filter case insensitive | 필터 대소문자 무시 |
| Remove filter | 필터 해제 후 결과 복원 |
| Blocked words property | 필터 목록 조회 |
| Filter applied on rebuild | 재구축 시 필터 적용 |
| Multiple words ordering | 다수 단어 빈도순 정렬 |

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide", Chapter 13
