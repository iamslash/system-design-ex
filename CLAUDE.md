# System Design Interview Examples

## 프로젝트 개요

System Design Interview (Alex Xu) 학습을 위한 실행 가능한 예제 모음.
각 챕터는 숫자 접두사 디렉토리로 구성되며, 개념 챕터 (README만)와 실행 가능한 챕터 (코드 + 테스트)로 나뉜다.

## 디렉토리 규칙

- `{번호}-{kebab-case-이름}/` 형식 (예: `05-design-a-rate-limiter/`)
- 개념 챕터: `README.md` 만 포함
- 실행 가능한 챕터: `README.md`, 소스 코드, 테스트, docker-compose (필요 시)

## 기술 스택

- **언어**: Python 3.11+
- **프레임워크**: FastAPI (API 서버)
- **인프라**: Redis, Docker Compose
- **테스트**: pytest
- **CLI**: argparse (외부 의존성 최소화)

## 코딩 컨벤션

- Python 타입 힌트 사용 (3.11+ 문법)
- 소스 코드 주석은 영어
- README 및 문서는 한국어
- 각 README에 핵심 구현 코드 스니펫 포함 (한국어 주석)
- 순수 알고리즘은 stdlib만 사용 (외부 의존성 없음)
- Docker가 필요한 프로젝트는 `docker-compose up --build`로 실행 가능해야 함
- 순수 Python 프로젝트는 `python scripts/demo.py`로 실행 가능해야 함

## 테스트

```bash
cd {챕터 디렉토리}
pip install -r requirements.txt  # 또는 node/requirements.txt
pytest tests/ -v
```

## 새 챕터 추가 시

1. `{번호}-{이름}/` 디렉토리 생성
2. README.md 작성 (한국어, 핵심 코드 포함)
3. 실행 가능한 예제 구현
4. 테스트 작성 및 통과 확인
5. 루트 README.md 목차 업데이트
