FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 비디오 저장 디렉토리 생성
RUN mkdir -p /data/videos

EXPOSE 8015

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8015"]
