from __future__ import annotations

import json
from pathlib import Path

import typer
import uvicorn

from .api import create_app
from .logging_config import configure_logging
from .services.consumer import TelemetryConsumer
from .services.producer import TelemetryProducer, generate_events

app = typer.Typer(no_args_is_help=True, add_completion=False)
seed_app = typer.Typer(help="Create local sample data")
app.add_typer(seed_app, name="seed")


@app.callback()
def main() -> None:
    configure_logging()


@seed_app.command("sample-data")
def seed_sample_data(output: Path, records: int = 200, vehicle_count: int = 12) -> None:
    rows = generate_events(vehicle_count=vehicle_count, records=records)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")
    typer.echo(f"wrote {records} records to {output}")


@app.command()
def produce(
    input: Path | None = typer.Option(None, help="Path to JSONL file to publish"),
    delay_seconds: float = typer.Option(0.0, help="Delay between published records"),
    generated_records: int = typer.Option(0, help="Generate records instead of reading from a file"),
    vehicle_count: int = typer.Option(10, help="Vehicle count when generating records"),
) -> None:
    producer = TelemetryProducer()
    if input is not None:
        producer.publish_file(input_path=input, delay_seconds=delay_seconds)
        return
    if generated_records <= 0:
        raise typer.BadParameter("provide --input or set --generated-records to a value > 0")
    producer.publish_generated(
        vehicle_count=vehicle_count,
        records=generated_records,
        delay_seconds=delay_seconds,
    )


@app.command()
def consume(
    max_batches: int = typer.Option(10, help="Number of poll/process cycles"),
    batch_size: int = typer.Option(100, help="Maximum messages per batch"),
    poll_seconds: float = typer.Option(1.0, help="Sleep time when no messages are available"),
) -> None:
    consumer = TelemetryConsumer()
    result = consumer.run(max_batches=max_batches, batch_size=batch_size, poll_seconds=poll_seconds)
    typer.echo(json.dumps(result, indent=2))


@app.command()
def api(
    host: str = typer.Option("0.0.0.0", help="Bind host"),
    port: int = typer.Option(8080, help="Bind port"),
) -> None:
    uvicorn.run(create_app(), host=host, port=port)
