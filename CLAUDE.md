# System Design Interview Examples

## 프로젝트 개요

System Design Interview (Alex Xu) 학습을 위한 실행 가능한 예제 모음.
각 챕터는 숫자 접두사 디렉토리로 구성되며, 개념 챕터 (README만)와 실행 가능한 챕터 (코드 + 테스트)로 나뉜다.

## 디렉토리 규칙

- `{번호}-{kebab-case-이름}/` 형식 (예: `05-design-a-rate-limiter/`)
- 챕터는 세 가지 아키타입으로 나뉘며, 일부 챕터는 하이브리드일 수 있다.

### 챕터 아키타입

| 아키타입 | 예상 파일 | 예시 |
|---|---|---|
| 개념 챕터 | `README.md` | `00`, `01`, `02`, `03`, `04`, `30` |
| 순수 Python 데모 | `README.md`, `src/`, `scripts/demo.py`, `tests/`, `requirements.txt` | `06`, `08`, `10`, `14`, `17`, `19`, `20`, `22`, `28`, `29` |
| 서비스/Docker 챕터 | `README.md`, `api/` 또는 `server/`, `docker-compose.yml`, `Dockerfile`, `tests/`, `requirements.txt`, `.env.example` (선택), `pytest.ini` (선택) | `05`, `07`, `09`, `11`~`13`, `15`, `16`, `18`, `21`, `23`~`27` |

## 기술 스택

- **언어**: Python 3.11+
- **프레임워크**: FastAPI (서비스/Docker 챕터의 API 서버)
- **인프라**: Redis, Docker Compose (서비스 챕터에서만 사용)
- **테스트**: pytest
- **CLI**: argparse (외부 의존성 최소화)

> 모든 챕터가 FastAPI + Redis + Docker Compose를 사용하지는 않는다.
> 순수 Python 데모 챕터는 stdlib 위주이며, Docker 없이 동작한다.

## 코딩 컨벤션

- Python 타입 힌트 사용 (3.11+ 문법)
- 소스 코드 주석은 영어
- README 및 문서는 한국어
- 실행 가능한 챕터의 README에 핵심 구현 코드 스니펫 포함 (한국어 주석). 개념 챕터는 서술 전용.
- 순수 알고리즘 챕터는 stdlib만 사용. 단, 웹 크롤러 등 특정 도메인 챕터는 외부 라이브러리 허용 (`requests`, `beautifulsoup4` 등).

### 실행 방법

| 아키타입 | 실행 명령 |
|---|---|
| 서비스/Docker 챕터 | `docker-compose up --build` |
| 순수 Python 데모 | `python scripts/demo.py` |
| 하이브리드 (예: `27`) | 두 가지 모두 지원 가능 |

## 테스트

의존성 파일 위치는 챕터 아키타입에 따라 다르다:

| 아키타입 | 의존성 위치 |
|---|---|
| 순수 Python 데모 | `requirements.txt` (최상위) |
| 서비스/Docker (FastAPI) | `api/requirements.txt` |
| 서비스/Docker (서버) | `server/requirements.txt` |
| Node.js 기반 | `node/requirements.txt` |

```bash
cd {챕터 디렉토리}
pip install -r requirements.txt          # 순수 Python 데모
pip install -r api/requirements.txt      # 서비스/Docker (FastAPI)
pip install -r server/requirements.txt   # 서비스/Docker (서버)
pytest tests/ -v
```

## 새 챕터 추가 시

1. `{번호}-{이름}/` 디렉토리 생성
2. README.md 작성 (한국어, 핵심 코드 포함)
3. 실행 가능한 예제 구현
4. 테스트 작성 및 통과 확인
5. 루트 README.md 목차 업데이트
