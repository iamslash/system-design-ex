# Design Distributed Email Service

분산 이메일 서비스는 수십억 사용자에게 이메일 송수신, 검색, 폴더 관리 기능을
제공하는 시스템이다. Gmail, Outlook 같은 서비스에서 영감을 받아 큐 기반
비동기 전송과 역인덱스 검색을 구현한다.

## 아키텍처

```
┌──────────────┐          ┌──────────────────────────────────────────────┐
│   Client     │  REST    │              API Server (FastAPI)            │
│  (Web/App)   │────────▶│                                              │
└──────────────┘          │  ┌──────────┐ ┌──────────┐ ┌──────────┐    │
                          │  │  Sender   │ │ Receiver │ │  Search  │    │
                          │  │ (queue)   │ │ (store)  │ │ (index)  │    │
                          │  └─────┬─────┘ └────▲─────┘ └──────────┘    │
                          │        │             │                       │
                          │  ┌─────▼─────────────┴─────┐                │
                          │  │     Redis (Storage)      │                │
                          │  │  - Email metadata        │                │
                          │  │  - Folder sets           │                │
                          │  │  - Outgoing queue        │                │
                          │  │  - Inverted index        │                │
                          │  └─────────┬───────────────┘                │
                          │            │                                 │
                          │  ┌─────────▼───────────────┐                │
                          │  │    SMTP Worker (Thread)  │                │
                          │  │  - Queue pop & deliver   │                │
                          │  └─────────────────────────┘                │
                          └──────────────────────────────────────────────┘
```

### 핵심 컴포넌트

| 컴포넌트 | 역할 |
|---------|------|
| **API Server** | FastAPI 기반 REST API, 이메일 CRUD 및 폴더/검색 |
| **Sender** | 이메일을 Redis 큐에 enqueue, 발신자 Sent 폴더에 저장 |
| **Receiver** | 수신자 Inbox 에 이메일 배달, 검색 인덱스 갱신 |
| **Search** | 역인덱스(inverted index) 기반 키워드 검색 |
| **Folder Manager** | 기본 폴더 (Inbox/Sent/Drafts/Trash) + 커스텀 폴더 |
| **SMTP Worker** | 큐에서 이메일을 pop 하여 수신자에게 배달 (시뮬레이션) |

## 이메일 프로토콜

### SMTP (Simple Mail Transfer Protocol)

```
발신 MUA ──SMTP──▶ 발신 MTA ──SMTP──▶ 수신 MTA
                                        │
                                        ▼
                               수신자 Mailbox
```

- **포트**: 25 (서버 간), 587 (클라이언트 → 서버, STARTTLS)
- **역할**: 이메일 전송 전용 프로토콜
- **흐름**: `HELO` → `MAIL FROM` → `RCPT TO` → `DATA` → `QUIT`

### POP3 vs IMAP

| | POP3 | IMAP |
|---|------|------|
| **동작** | 서버에서 다운로드 후 삭제 | 서버에 메일 유지, 동기화 |
| **폴더** | 단일 (Inbox) | 다중 폴더 지원 |
| **검색** | 클라이언트 로컬 | 서버측 검색 가능 |
| **포트** | 110 / 995 (SSL) | 143 / 993 (SSL) |
| **적합** | 단일 디바이스 | 멀티 디바이스 (현대 표준) |

## 이메일 전송 흐름

### Send Flow (큐 기반 비동기 전송)

```python
def send_email(r: Redis, *, from_addr: str, to_addrs: list[str],
               subject: str = "", body: str = "",
               attachments: list[Attachment] | None = None,
               in_reply_to: str | None = None) -> Email:
    """이메일을 Redis 큐에 enqueue 하고 발신자 Sent 폴더에 저장."""
    # 스레드 결정: 기존 스레드 이어가기 or 새 스레드
    thread_id = None
    if in_reply_to:
        parent = r.get(f"email:msg:{in_reply_to}")
        if parent:
            thread_id = json.loads(parent).get("thread_id")
    if not thread_id:
        thread_id = uuid.uuid4().hex  # 새 스레드 시작

    email = Email(from_addr=from_addr, to_addrs=to_addrs,
                  subject=subject, body=body, thread_id=thread_id,
                  folder=FolderType.SENT, is_read=True)

    pipe = r.pipeline()
    pipe.set(f"email:msg:{email.email_id}", email.model_dump_json())
    pipe.sadd(f"email:folder:{from_addr}:sent", email.email_id)
    pipe.rpush(f"email:thread:{thread_id}", email.email_id)
    pipe.rpush("email:outgoing_queue", email.model_dump_json())  # 큐에 추가
    pipe.execute()
    return email
```

**핵심 포인트**:
- `rpush` 로 Redis List 에 enqueue → SMTP Worker 가 비동기 처리
- Pipeline 으로 원자적 저장 (metadata + folder + thread + queue)
- `in_reply_to` 가 있으면 부모의 `thread_id` 를 상속

### Receive Flow (SMTP Worker 배달)

```python
def process_one(r: Redis) -> bool:
    """큐에서 이메일 하나를 pop 하여 수신자 Inbox 에 배달."""
    raw = r.lpop("email:outgoing_queue")  # FIFO pop
    if raw is None:
        return False
    email = Email.model_validate_json(raw)
    deliver_to_inbox(r, email)   # 수신자별 Inbox 저장
    index_email(r, email, ...)   # 검색 인덱스 갱신
    return True

def deliver_to_inbox(r: Redis, email: Email) -> list[str]:
    """모든 수신자(TO + CC) Inbox 에 이메일 저장."""
    for recipient in set(email.to_addrs + email.cc_addrs):
        inbox_email = email.model_copy(
            update={"is_read": False, "folder": FolderType.INBOX}
        )
        pipe = r.pipeline()
        pipe.set(f"email:msg:{inbox_email.email_id}",
                 inbox_email.model_dump_json())
        pipe.sadd(f"email:folder:{recipient}:inbox",
                  inbox_email.email_id)
        pipe.execute()
```

## 데이터 모델

### Redis Key 설계

```
email:msg:{email_id}                    → JSON (이메일 메타데이터)
email:folder:{user}:{folder_name}       → SET of email_ids
email:thread:{thread_id}                → LIST of email_ids (시간순)
email:attachment:{email_id}:{att_id}    → JSON (첨부파일 메타)
email:outgoing_queue                    → LIST (발송 대기 큐)
email:index:{user}:{token}             → SET of email_ids (역인덱스)
email:custom_folders:{user}             → SET of folder_names
email:delivery_log                      → LIST (배달 로그)
```

### Email 스키마

```python
class Email(BaseModel):
    email_id: str          # UUID hex
    from_addr: str         # 발신자
    to_addrs: list[str]    # 수신자 목록
    cc_addrs: list[str]    # CC 목록
    bcc_addrs: list[str]   # BCC 목록
    subject: str           # 제목
    body: str              # 본문
    attachments: list[Attachment]
    thread_id: str | None  # 스레드 ID
    in_reply_to: str | None  # 답장 대상 email_id
    is_read: bool          # 읽음 여부
    folder: FolderType     # 현재 폴더 (inbox/sent/drafts/trash)
    created_at: str        # ISO 8601 타임스탬프
```

## 검색: 역인덱스 (Inverted Index)

### 원리

```
"quarterly" → {email_001, email_042, email_187}
"report"    → {email_001, email_099}
"budget"    → {email_042, email_099}

검색: "quarterly report"
→ SINTER("quarterly", "report") = {email_001}   # AND 의미론
```

### 구현

```python
def _tokenize(text: str) -> set[str]:
    """텍스트를 소문자 알파뉴메릭 토큰으로 분할."""
    return {t for t in re.split(r"\W+", text.lower()) if t}

def index_email(r: Redis, email: Email, user: str) -> None:
    """제목과 본문의 토큰을 역인덱스에 추가."""
    tokens = _tokenize(email.subject) | _tokenize(email.body)
    pipe = r.pipeline()
    for token in tokens:
        pipe.sadd(f"email:index:{user}:{token}", email.email_id)
    pipe.execute()

def search_emails(r: Redis, user: str, query: str) -> list[str]:
    """AND 의미론으로 이메일 검색. SINTER 활용."""
    tokens = _tokenize(query)
    keys = [f"email:index:{user}:{t}" for t in tokens]
    if len(keys) == 1:
        result = r.smembers(keys[0])
    else:
        result = r.sinter(*keys)  # 모든 토큰이 포함된 이메일만
    return list(result)
```

### Inverted Index vs Elasticsearch

| | 단순 역인덱스 (Redis) | Elasticsearch |
|---|---|---|
| **장점** | 구현 간단, 지연 낮음 | 풍부한 쿼리 (fuzzy, range, aggregation) |
| **단점** | 정확 매칭만, 형태소 분석 없음 | 운영 복잡도, 리소스 |
| **적합** | 프로토타입, 소규모 | 프로덕션, 대규모 |
| **인덱싱** | `SADD` O(1) | 역인덱스 + BKD Tree + 캐시 |
| **검색** | `SINTER` O(N*M) | TF-IDF / BM25 랭킹 |

프로덕션 이메일 시스템은 Elasticsearch 를 사용하되, 핵심 원리는 동일하다:
토큰화 → 역인덱스 매핑 → 교집합/합집합 연산.

## 폴더 관리

```python
DEFAULT_FOLDERS = [FolderType.INBOX, FolderType.SENT,
                   FolderType.DRAFTS, FolderType.TRASH]

def move_email(r: Redis, user: str, email_id: str,
               from_folder: str, to_folder: str) -> bool:
    """이메일을 폴더 간 이동. Redis SET 의 SREM + SADD."""
    if not r.sismember(f"email:folder:{user}:{from_folder}", email_id):
        return False
    pipe = r.pipeline()
    pipe.srem(f"email:folder:{user}:{from_folder}", email_id)
    pipe.sadd(f"email:folder:{user}:{to_folder}", email_id)
    pipe.execute()
    return True

def delete_email(r: Redis, user: str, email_id: str,
                 current_folder: str) -> bool:
    """Trash 가 아니면 Trash 로 이동, Trash 면 영구 삭제."""
    if current_folder == "trash":
        r.srem(f"email:folder:{user}:trash", email_id)
        r.delete(f"email:msg:{email_id}")  # 영구 삭제
        return True
    return move_email(r, user, email_id, current_folder, "trash")
```

## 이메일 스레딩 (Conversation Threading)

```
┌─────────────────────────────────────────────┐
│ Thread: thread_abc123                       │
│                                             │
│  [1] Alice → Bob: "Project kickoff"         │
│  [2] Bob → Alice: "Re: Project kickoff"     │
│  [3] Alice → Bob: "Re: Re: Project kickoff" │
│                                             │
│  Redis: email:thread:thread_abc123          │
│         → LIST [email_001, email_002, ...]  │
└─────────────────────────────────────────────┘
```

- **새 이메일**: 새로운 `thread_id` (UUID) 할당
- **답장 (`in_reply_to`)**: 부모 이메일의 `thread_id` 를 상속
- **조회**: `LRANGE email:thread:{thread_id} 0 -1` → 시간순 이메일 목록

실제 시스템에서는 `Message-ID`, `In-Reply-To`, `References` 헤더로 스레딩한다.
Gmail 은 제목 기반 그룹핑도 추가로 사용한다.

## 이메일 전달 보장 (Deliverability)

### SPF (Sender Policy Framework)

```
DNS TXT: v=spf1 include:_spf.google.com ~all
```

발신 서버 IP 가 도메인의 SPF 레코드에 허용된 IP 인지 검증한다.

### DKIM (DomainKeys Identified Mail)

```
발신 서버: 이메일 헤더에 디지털 서명 추가
수신 서버: DNS 에서 공개키 조회 → 서명 검증
```

이메일이 전송 중 변조되지 않았음을 증명한다.

### DMARC (Domain-based Message Authentication)

```
DNS TXT: v=DMARC1; p=reject; rua=mailto:dmarc@example.com
```

SPF + DKIM 결과를 종합하여 정책 (none/quarantine/reject) 을 적용한다.
수신 서버는 DMARC 정책에 따라 스팸 처리하거나 거부한다.

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│   SPF    │     │   DKIM   │     │  DMARC   │
│ IP 검증  │────▶│ 서명 검증 │────▶│ 정책 적용 │
└──────────┘     └──────────┘     └──────────┘
```

## 실행 방법

### Docker Compose

```bash
cd 24-design-distributed-email-service
docker-compose up --build
```

API: `http://localhost:8024`

### 테스트

```bash
cd 24-design-distributed-email-service
pip install -r api/requirements.txt
pytest tests/ -v
```

### CLI

```bash
# 이메일 전송
python scripts/cli.py send --from alice@example.com --to bob@example.com \
  --subject "Hello" --body "Hi Bob!"

# 수동 배달 트리거
python scripts/cli.py deliver

# 받은편지함 확인
python scripts/cli.py inbox --user bob@example.com

# 이메일 검색
python scripts/cli.py search --user bob@example.com --query "hello"

# 스레드 조회
python scripts/cli.py thread <thread_id>
```

### API 엔드포인트

| Method | Path | 설명 |
|--------|------|------|
| POST | `/api/email/send` | 이메일 전송 (큐 enqueue) |
| POST | `/api/email/deliver` | 수동 배달 트리거 |
| GET | `/api/email/{email_id}` | 이메일 조회 |
| POST | `/api/email/{email_id}/read` | 읽음 표시 |
| POST | `/api/email/{email_id}/unread` | 안읽음 표시 |
| GET | `/api/email/{email_id}/attachment/{att_id}` | 첨부파일 조회 |
| GET | `/api/thread/{thread_id}` | 스레드 조회 |
| GET | `/api/folders/{user}` | 폴더 목록 |
| GET | `/api/folders/{user}/{folder}` | 폴더 이메일 목록 |
| GET | `/api/folders/{user}/{folder}/unread` | 안읽은 수 |
| POST | `/api/folders/create` | 커스텀 폴더 생성 |
| POST | `/api/email/move` | 이메일 폴더 이동 |
| DELETE | `/api/email/{user}/{email_id}` | 이메일 삭제 |
| POST | `/api/email/search` | 키워드 검색 |
| GET | `/health` | 헬스 체크 |
