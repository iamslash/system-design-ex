# Design A Web Crawler

웹 크롤러(Web Crawler)는 시드(seed) URL 에서 시작하여 웹 페이지를 체계적으로
탐색하고 수집하는 소프트웨어다. 검색 엔진 인덱싱, 웹 아카이빙, 데이터 마이닝 등에
사용된다. 이 장에서는 BFS 기반 크롤러의 핵심 컴포넌트를 Python 으로 구현한다.

---

## 아키텍처

```
Seed URLs
   │
   ▼
┌──────────────────────────────────────────────────────────────────┐
│                         URL Frontier                             │
│  ┌─────────────────────┐    ┌─────────────────────────────────┐  │
│  │  Front Queues        │    │  Back Queues                    │  │
│  │  (Priority)          │    │  (Politeness: per-host delay)   │  │
│  │                      │    │                                 │  │
│  │  P0: [seed urls]     │ →  │  host-a queue ──→ delay 1s      │  │
│  │  P1: [same-domain]   │    │  host-b queue ──→ delay 1s      │  │
│  │  P5: [cross-domain]  │    │  host-c queue ──→ delay 1s      │  │
│  └─────────────────────┘    └─────────────────────────────────┘  │
└──────────────────────┬───────────────────────────────────────────┘
                       │  next URL
                       ▼
              ┌─────────────────┐     ┌──────────────────┐
              │  robots.txt     │────▶│  Allowed?         │
              │  Parser/Cache   │     │  Yes → continue   │
              └─────────────────┘     │  No  → skip       │
                                      └────────┬─────────┘
                                               │
                                               ▼
                                      ┌─────────────────┐
                                      │  HTML Downloader │
                                      │  (requests)      │
                                      └────────┬────────┘
                                               │ HTML
                                               ▼
                                      ┌─────────────────┐
                                      │  Content Seen?   │──── Yes → skip (중복)
                                      │  (MD5 hash)      │
                                      └────────┬────────┘
                                               │ No (새 컨텐츠)
                                               ▼
                                      ┌─────────────────┐
                                      │  Content Parser  │
                                      │  (BeautifulSoup) │
                                      └────────┬────────┘
                                               │ extracted links
                                               ▼
                                      ┌─────────────────┐
                                      │  Link Extractor  │
                                      │  & URL Filter    │
                                      └────────┬────────┘
                                               │ new URLs
                                               ▼
                                      ┌─────────────────┐
                                      │  URL Seen?       │──── Yes → skip (이미 봄)
                                      │  (set / bloom)   │
                                      └────────┬────────┘
                                               │ No (새 URL)
                                               ▼
                                         URL Frontier 에 추가
                                         (loop back ↑)
```

### 요청 흐름

1. Seed URL 을 URL Frontier 에 추가한다.
2. Frontier 에서 우선순위가 가장 높은 URL 을 꺼낸다.
3. robots.txt 를 확인하여 크롤링이 허용되는지 검사한다.
4. Politeness delay 를 준수한 후 페이지를 다운로드한다.
5. Content fingerprint (MD5) 로 중복 컨텐츠를 탐지한다.
6. HTML 을 파싱하여 링크를 추출하고, URL filter 를 적용한다.
7. 새로운 URL 만 URL Seen 을 거쳐 Frontier 에 추가한다.
8. Frontier 가 비거나 최대 페이지 수에 도달할 때까지 반복한다.

---

## Back-of-the-Envelope Estimation

| 항목 | 수치 |
|------|------|
| 크롤링 대상 | 1B (10억) 페이지/월 |
| QPS | 1B / (30 x 24 x 3600) ≈ **~400 QPS** |
| Peak QPS | ~400 x 2 = **~800 QPS** |
| 평균 페이지 크기 | 500 KB |
| 월간 저장량 | 1B x 500 KB = **~500 TB/월** |
| 5년 저장량 | 500 TB x 12 x 5 = **~30 PB** |

---

## BFS vs DFS

| 항목 | BFS (너비 우선) | DFS (깊이 우선) |
|------|----------------|----------------|
| 탐색 순서 | 같은 깊이의 페이지를 먼저 | 한 경로를 끝까지 먼저 |
| 장점 | 중요한 페이지를 빨리 발견, 깊이 제어 용이 | 구현 간단, 메모리 효율적 |
| 단점 | 메모리 사용량 높음 (큐) | 무한 루프 위험, 깊은 경로에 갇힘 |
| **웹 크롤러** | **표준 선택** | 부적합 |

웹 크롤러는 **BFS** 를 사용한다. 시드 URL 에 가까운 (깊이가 낮은) 페이지가
일반적으로 더 중요하기 때문이다.

---

## URL Frontier 설계

URL Frontier 는 **두 가지 역할**을 한다:

1. **Front Queues (Priority)**: URL 의 중요도에 따라 우선순위를 부여
   - Priority 0: seed URLs (가장 중요)
   - Priority 3: 같은 도메인 내 링크
   - Priority 7: 외부 도메인 링크

2. **Back Queues (Politeness)**: 같은 호스트에 대한 요청 간격을 조절
   - 호스트별 마지막 접근 시간을 기록
   - 설정된 delay (기본 1초) 가 지나야 다음 요청 가능

---

## 핵심 구현

### 1. URL Frontier (`src/frontier.py`)

Priority queue (min-heap) + per-host politeness delay:

```python
@dataclass(order=True)
class FrontierEntry:
    """Frontier 큐 항목.

    priority 가 낮을수록 먼저 처리된다 (min-heap).
    """

    priority: int
    url: str = field(compare=False)
    depth: int = field(compare=False, default=0)
    _counter: int = field(compare=True, default=0, repr=False)


class URLFrontier:
    """URL Frontier with priority ordering and per-host politeness.

    - Priority queue: 낮은 priority 값이 먼저 크롤링됨
    - Politeness: 같은 호스트에 대한 연속 요청 사이에 최소 delay 유지
    - Max depth: 설정된 최대 깊이를 초과하는 URL 은 추가하지 않음
    """

    def __init__(
        self,
        politeness_delay: float = 1.0,
        max_depth: int = 3,
    ) -> None:
        self._heap: list[FrontierEntry] = []
        self._politeness_delay = politeness_delay
        self._max_depth = max_depth
        self._last_access: dict[str, float] = {}  # host -> last access time
        self._counter = 0  # tie-breaker for heap (FIFO within same priority)

    def add(self, url: str, priority: int = 5, depth: int = 0) -> bool:
        """URL 을 Frontier 에 추가한다. max_depth 초과 시 False 반환."""
        if depth > self._max_depth:
            return False

        entry = FrontierEntry(
            priority=priority,
            url=url,
            depth=depth,
            _counter=self._counter,
        )
        self._counter += 1
        heapq.heappush(self._heap, entry)
        return True

    def get_next(self) -> FrontierEntry | None:
        """다음에 크롤링할 URL 을 반환한다."""
        if not self._heap:
            return None
        return heapq.heappop(self._heap)

    def get_wait_time(self, url: str) -> float:
        """해당 URL 의 호스트에 대해 대기해야 할 시간(초)을 반환한다."""
        host = self._get_host(url)
        last = self._last_access.get(host, 0.0)
        elapsed = time.monotonic() - last
        remaining = self._politeness_delay - elapsed
        return max(0.0, remaining)

    def record_access(self, url: str) -> None:
        """호스트에 대한 접근 시간을 기록한다."""
        host = self._get_host(url)
        self._last_access[host] = time.monotonic()
```

### 2. HTML Parser + Link Extractor (`src/parser.py`)

BeautifulSoup 으로 HTML 을 파싱하고 링크를 추출한다:

```python
def extract_links(html: str, base_url: str) -> list[str]:
    """HTML 에서 모든 <a href="..."> 링크를 추출한다.

    - 상대 URL → 절대 URL 변환 (urljoin)
    - fragment-only (#), javascript:, mailto: 링크 제외
    - HTTP/HTTPS 스킴만 포함
    - 중복 제거 (순서 유지)
    """
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []

    for anchor in soup.find_all("a", href=True):
        href: str = anchor["href"].strip()

        # fragment-only 링크 (#section) 무시
        if href.startswith("#"):
            continue

        # javascript:, mailto:, tel: 등 비-HTTP 스킴 무시
        if href.startswith(("javascript:", "mailto:", "tel:", "data:")):
            continue

        # 상대 URL → 절대 URL 변환
        absolute = urljoin(base_url, href)

        # URL 정규화 및 유효성 검사
        normalized = _normalize_url(absolute)
        if normalized and _is_valid_http(normalized):
            urls.append(normalized)

    # 중복 제거 (순서 유지)
    seen: set[str] = set()
    unique: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)

    return unique
```

### 3. URL Dedup / Content Seen (`src/dedup.py`)

**URL Seen** — set 기반 URL 중복 방지:

```python
class URLSeen:
    """URL 중복 방문 방지. 프로덕션에서는 Bloom filter / Redis set 으로 대체."""

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def add(self, url: str) -> None:
        self._seen.add(self._normalize(url))

    def is_seen(self, url: str) -> bool:
        return self._normalize(url) in self._seen

    @staticmethod
    def _normalize(url: str) -> str:
        """URL 정규화: trailing slash 제거, fragment 제거."""
        if "#" in url:
            url = url[: url.index("#")]
        if url.endswith("/") and len(url) > 1:
            url = url.rstrip("/")
        return url.lower()
```

**Content Seen** — MD5 hash 기반 컨텐츠 중복 탐지:

```python
class ContentSeen:
    """컨텐츠 중복 탐지. 같은 내용의 페이지가 다른 URL 에 존재할 수 있다."""

    def __init__(self) -> None:
        self._fingerprints: set[str] = set()

    def is_duplicate(self, content: str) -> bool:
        fp = hashlib.md5(content.encode("utf-8")).hexdigest()
        if fp in self._fingerprints:
            return True
        self._fingerprints.add(fp)
        return False
```

**Bloom Filter** — 교육용 구현 (프로덕션에서 URL Seen 대체):

```python
class SimpleBloomFilter:
    """Educational Bloom filter.

    "아마도 존재함" 또는 "확실히 없음" 을 O(1) 에 판별.
    False positive 는 있지만 false negative 는 없다.
    """

    def __init__(self, size: int = 1_000_000, num_hashes: int = 5) -> None:
        self._size = size
        self._num_hashes = num_hashes
        self._bit_array = bytearray(size)

    def _hashes(self, item: str) -> list[int]:
        """Double hashing: h(i) = (h1 + i * h2) mod size"""
        h1 = int(hashlib.md5(item.encode()).hexdigest(), 16)
        h2 = int(hashlib.sha1(item.encode()).hexdigest(), 16)
        return [(h1 + i * h2) % self._size for i in range(self._num_hashes)]

    def add(self, item: str) -> None:
        for idx in self._hashes(item):
            self._bit_array[idx] = 1

    def might_contain(self, item: str) -> bool:
        return all(self._bit_array[idx] == 1 for idx in self._hashes(item))
```

### 4. Robots.txt Parser (`src/robots_parser.py`)

`urllib.robotparser` 를 사용하여 robots.txt 를 파싱하고 캐싱한다:

```python
class RobotsChecker:
    """robots.txt parser with per-domain caching."""

    def __init__(self, user_agent: str = "SystemDesignCrawler", timeout: float = 5.0) -> None:
        self._user_agent = user_agent
        self._timeout = timeout
        self._cache: dict[str, urllib.robotparser.RobotFileParser] = {}

    def is_allowed(self, url: str) -> bool:
        """URL 에 대한 크롤링이 robots.txt 에 의해 허용되는지 확인한다."""
        parser = self._get_parser(url)
        if parser is None:
            return True  # robots.txt 를 가져올 수 없으면 허용
        return parser.can_fetch(self._user_agent, url)

    def _fetch_robots(self, robots_url: str) -> RobotFileParser | None:
        """robots.txt 를 다운로드하고 파싱한다."""
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        try:
            resp = requests.get(robots_url, timeout=self._timeout)
            if resp.status_code == 200:
                rp.parse(resp.text.splitlines())
                return rp
            elif resp.status_code in (401, 403):
                rp.parse(["User-agent: *", "Disallow: /"])  # 모든 URL 차단
                return rp
            else:
                return None  # 404 등: robots.txt 없음 → 모두 허용
        except (requests.RequestException, OSError):
            return None
```

### 5. BFS Crawler (`src/crawler.py`)

모든 컴포넌트를 조합한 메인 크롤 루프:

```python
class WebCrawler:
    """BFS web crawler.

    Seed URL 목록에서 시작하여 너비 우선으로 페이지를 탐색한다.
    """

    def __init__(
        self,
        seed_urls: list[str],
        max_pages: int = 50,
        max_depth: int = 3,
        politeness_delay: float = 1.0,
        request_timeout: float = 10.0,
        user_agent: str = "SystemDesignCrawler/1.0",
    ) -> None:
        self._frontier = URLFrontier(politeness_delay=politeness_delay, max_depth=max_depth)
        self._url_seen = URLSeen()
        self._content_seen = ContentSeen()
        self._robots = RobotsChecker(user_agent=user_agent)

        # Seed URL 을 frontier 에 추가
        for url in seed_urls:
            if not self._url_seen.is_seen(url):
                self._url_seen.add(url)
                self._frontier.add(url, priority=0, depth=0)

    def crawl(self) -> tuple[list[CrawlResult], CrawlStats]:
        """BFS 크롤링을 실행한다."""
        results: list[CrawlResult] = []
        stats = CrawlStats()

        while not self._frontier.is_empty and stats.pages_crawled < self._max_pages:
            entry = self._frontier.get_next()
            url, depth = entry.url, entry.depth

            # 1. robots.txt 확인
            if not self._robots.is_allowed(url):
                stats.robots_blocked += 1
                continue

            # 2. Politeness delay 대기
            wait = self._frontier.get_wait_time(url)
            if wait > 0:
                time.sleep(wait)

            # 3. 페이지 다운로드
            result = self._download(url, depth)
            self._frontier.record_access(url)

            # 4. 컨텐츠 중복 확인 → 5. 파싱 & 링크 추출 → 6. 새 URL frontier 추가
            ...

        return results, stats
```

---

## Politeness (예의)

웹 크롤러는 대상 서버에 과부하를 주지 않도록 **예의**를 지켜야 한다:

- **robots.txt 준수**: 사이트 소유자가 허용하지 않는 경로는 크롤링하지 않음
- **Per-host delay**: 같은 호스트에 대해 일정 간격(기본 1초) 이상 대기
- **User-Agent 식별**: 명확한 User-Agent 헤더로 크롤러를 식별
- **타임아웃 설정**: 응답 없는 서버에 무한 대기하지 않음

## Robustness (견고성)

- **예외 처리**: 네트워크 오류, 타임아웃, 잘못된 HTML 등 다양한 예외 상황 처리
- **컨텐츠 중복 탐지**: MD5 해시로 동일 컨텐츠를 가진 다른 URL 탐지
- **최대 깊이 제한**: spider trap (무한 깊이 URL) 방지
- **Graceful shutdown**: KeyboardInterrupt 시 정상 종료

## Extensibility (확장성)

프로덕션 크롤러로 확장할 때 고려할 사항:

| 컴포넌트 | 현재 구현 | 프로덕션 확장 |
|----------|----------|-------------|
| URL Seen | Python set | Bloom filter + Redis |
| Content Seen | MD5 hash | SimHash (유사 컨텐츠 탐지) |
| Frontier | 단일 프로세스 heapq | 분산 메시지 큐 (Kafka, RabbitMQ) |
| Downloader | requests (동기) | aiohttp (비동기) + 멀티 워커 |
| Storage | 메모리 | S3, HDFS |
| DNS | 시스템 기본 | 로컬 DNS 캐시 |

---

## 실행 방법

### 사전 조건

- Python 3.11 이상

### 의존성 설치

```bash
cd 10-design-a-web-crawler
pip install -r requirements.txt
```

### 데모 실행

```bash
python scripts/demo.py
```

커맨드라인 옵션:

```bash
# 기본: quotes.toscrape.com 에서 20 페이지 크롤링
python scripts/demo.py

# 다른 사이트, 10 페이지, 깊이 1
python scripts/demo.py --seed https://books.toscrape.com/ --max-pages 10 --max-depth 1

# 빠른 크롤링 (0.3초 딜레이)
python scripts/demo.py --delay 0.3 --max-pages 5
```

### 테스트 실행

```bash
pip install -r requirements.txt
pytest tests/ -v
```

---

## 샘플 출력

```
Web Crawler Demo
================

  Seed URL       : https://quotes.toscrape.com/
  Max Pages      : 20
  Max Depth      : 2
  Politeness Delay: 0.5s

======================================================================
  Crawling...
======================================================================

  [  1] 200  depth=0  links=52   "Quotes to Scrape"
        https://quotes.toscrape.com/
  [  2] 200  depth=1  links=52   "Quotes to Scrape"
        https://quotes.toscrape.com/page/2/
  [  3] 200  depth=1  links=11   "Top Ten tags"
        https://quotes.toscrape.com/tag/love/
  [  4] 200  depth=1  links=11   "Top Ten tags"
        https://quotes.toscrape.com/tag/inspirational/
  ...
  [ 20] 200  depth=2  links=8  [DUP]
        https://quotes.toscrape.com/tag/humor/page/1/

======================================================================
  Summary
======================================================================

  Pages crawled      : 20
  Pages failed       : 0
  Content duplicates : 2
  Robots.txt blocked : 0
  URLs discovered    : 147
  Elapsed time       : 11.23s
  Avg time per page  : 0.56s

======================================================================
  Successfully Crawled URLs
======================================================================

    depth=0  https://quotes.toscrape.com/
    depth=1  https://quotes.toscrape.com/page/2/
    depth=1  https://quotes.toscrape.com/tag/love/
    ...

Done.
```

> 참고: 실제 출력 값은 실행 시점과 네트워크 상태에 따라 달라진다.
> 대상 사이트가 이용 불가능한 경우 `--seed` 옵션으로 다른 사이트를 지정할 수 있다.

---

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide", Chapter 9
