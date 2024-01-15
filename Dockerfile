FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update     && apt-get install -y --no-install-recommends build-essential curl     && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
COPY docs ./docs
COPY sql ./sql
COPY sample_data ./sample_data
COPY .env.example ./

RUN pip install --no-cache-dir -e .

RUN mkdir -p /app/data/broker /app/data/lakehouse

EXPOSE 8080

CMD ["fleet-telemetry", "api", "--host", "0.0.0.0", "--port", "8080"]
