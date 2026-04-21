# MyTel

A fleet telemetry data platform for ingesting, validating, enriching, and serving vehicle telemetry data.

## Overview

MyTel is a data engineering project built around connected vehicle telemetry. It simulates telemetry events, processes them through a pipeline, stores outputs in bronze, silver, gold, and quarantine layers, and exposes monitoring endpoints through a FastAPI service.

This project is designed to demonstrate a practical workflow for telemetry and IoT-style data systems, including ingestion, validation, enrichment, storage, analytics, and API access.

## Features

- Telemetry ingestion and processing
- Data quality validation
- Bronze / Silver / Gold / Quarantine storage layers
- FastAPI monitoring API
- SQL analytics examples
- Docker Compose workflow
- Test coverage for pipeline and API behavior

## Tech Stack

- Python
- FastAPI
- DuckDB
- Docker
- Docker Compose
- Redpanda / Kafka-style messaging

## Project Structure

```text
sample_data/   # sample telemetry events
sql/           # analytics queries
src/           # application code
tests/         # test suite
Dockerfile
docker-compose.yml
Makefile
```

## Quick Start

Run the full local stack with Docker Compose:

```bash
docker compose up --build
```

The API will be available at:

```text
http://localhost:8080
```

## API Endpoints

- `GET /health`
- `GET /quality/summary`
- `GET /predictions/latest`
- `GET /metrics`

## Why This Project

MyTel demonstrates how raw telemetry data can be turned into a practical data platform with ingestion, validation, enrichment, curated storage, observability, and API access.

It is intended as a portfolio project for data engineering, telemetry pipelines, IoT systems, and backend analytics workflows.

## License

MIT
