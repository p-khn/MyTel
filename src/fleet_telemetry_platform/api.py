from __future__ import annotations

from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from .storage import LakehouseStore


def create_app(store: LakehouseStore | None = None) -> FastAPI:
    app = FastAPI(title="Fleet Telemetry Monitoring API", version="0.1.0")
    lakehouse = store or LakehouseStore()

    @app.get("/health")
    def health() -> dict:
        summary = lakehouse.quality_summary()
        return {"status": "ok", **summary}

    @app.get("/predictions/latest")
    def latest_predictions(limit: int = 20) -> list[dict]:
        return lakehouse.latest_predictions(limit=limit)

    @app.get("/quality/summary")
    def quality_summary() -> dict:
        return lakehouse.quality_summary()

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app
