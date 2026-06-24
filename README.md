# stream2pg

Kafka to PostgreSQL sink using Spark Structured Streaming.

## Installation

```bash
pip install -e .
```

## Usage

```python
from stream2pg import Stream2Pg

config = {
    "postgres": {
        "host": "localhost",
        "port": 5432,
        "dbname": "mydb",
        "user": "postgres",
        "password": "secret",
    },
    "kafka": {
        "bootstrap_servers": "localhost:9092",
        "subscribe_pattern": "mobility-.*",
        "starting_offsets": "earliest",
        "fail_on_data_loss": "false",
    },
    "processing": {
        "error_strategy": "skip_on_error",
        "checkpoint_location": "./checkpoints/kafka_to_postgres",
    },
}

def on_metrics(batch_id, row_count, inserted_count, skipped_count, elapsed_ms):
    print(f"Batch {batch_id}: {inserted_count} inserted, {skipped_count} skipped in {elapsed_ms}ms")

sink = Stream2Pg(config, on_metrics=on_metrics)
sink.run()
```

Or use the functional API:

```python
from stream2pg import run

run(config, on_metrics=on_metrics)
```

## Configuration

| Key | Required | Description |
|-----|----------|-------------|
| `postgres` | Yes | PostgreSQL connection parameters |
| `kafka` | Yes | Kafka consumer configuration |
| `processing` | Yes | Processing options |

### Error Strategy

- `RAISE` - Fail batch on any error
- `SKIP_ON_ERROR` - Skip records that fail, commit remaining

## API

### `Stream2Pg(config, on_metrics=None)`

Create a new sink instance.

- `config` - Configuration dictionary
- `on_metrics` - Optional callback `(batch_id, row_count, inserted_count, skipped_count, elapsed_ms)`

### `Stream2Pg.run()`

Start the Kafka to PostgreSQL streaming pipeline.

### `run(config, on_metrics=None)`

Functional API wrapper.

### `stream2pg.errors.ConfigurationError`

Exception raised for invalid configuration.
