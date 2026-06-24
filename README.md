<div align="center">

# stream2pg

**Automatically stream JSON data from Kafka into PostgreSQL using Spark Structured Streaming.**

Create tables. Evolve schemas. Load data.

<a href="https://pypi.org/project/stream2pg/">
  <img src="https://img.shields.io/pypi/v/stream2pg.svg?cacheSeconds=60" alt="PyPI">
</a>
<a href="https://pypi.org/project/stream2pg/">
  <img src="https://img.shields.io/pypi/pyversions/stream2pg.svg?cacheSeconds=60" alt="Python">
</a>
<a href="LICENSE">
  <img src="https://img.shields.io/github/license/shndap/stream2pg">
</a>

</div>

---

## Features

- 🚀 Kafka → PostgreSQL streaming
- 🔄 Automatic schema evolution
- 📦 Spark Structured Streaming
- 🐘 PostgreSQL integration
- 📊 Metrics callbacks
- ⚡ Minimal configuration



## How It Works

Given Kafka messages like:

```json
{
  "vehicle_id": 42,
  "speed": 18.7,
  "timestamp": "2025-01-01T12:00:00Z"
}
```

`stream2pg` will:

1. Subscribe to matching Kafka topics
2. Create PostgreSQL tables automatically
3. Infer column types
4. Add new columns when unseen fields appear
5. Insert records into PostgreSQL

No manual DDL required.



## Installation

### Development

```bash
pip install -e .
```

### From PyPI

```bash
pip install stream2pg
```



## Quick Start

```python
from stream2pg import Stream2Pg

config = {
    "postgres": {
        "host": "localhost",
        "port": 5432,
        "dbname": "mobility",
        "user": "postgres",
        "password": "secret",
    },
    "kafka": {
        "bootstrap_servers": "localhost:9092",
        "topic_prefix": "mobility-",
        "starting_offsets": "earliest",
        "fail_on_data_loss": "false",
    },
    "processing": {
        "error_strategy": "SKIP_ON_ERROR",
        "checkpoint_location": "./checkpoints/kafka_to_postgres",
    },
}

sink = Stream2Pg(config)
sink.run()
```



## Metrics

A metrics callback can be provided to observe batch execution.

```python
def on_metrics(
    batch_id,
    row_count,
    inserted_count,
    skipped_count,
    elapsed_ms,
):
    print(
        f"Batch {batch_id}: "
        f"{inserted_count} inserted, "
        f"{skipped_count} skipped "
        f"in {elapsed_ms} ms"
    )

sink = Stream2Pg(config, on_metrics=on_metrics)
sink.run()
```



## Functional API

If you prefer not to instantiate the class directly:

```python
from stream2pg import run

run(config)
```



## Configuration

### PostgreSQL

```python
{
    "host": "localhost",
    "port": 5432,
    "dbname": "mobility",
    "user": "postgres",
    "password": "secret",
}
```

### Kafka

```python
{
    "bootstrap_servers": "localhost:9092",
    "topic_prefix": "mobility-",
    "subscribe_pattern": "mobility-.*",
    "starting_offsets": "earliest",
    "fail_on_data_loss": "false",
}
```

| Parameter          | Description                                              |
|--------------------|----------------------------------------------------------|
| `bootstrap_servers`| Kafka broker addresses (default: `localhost:9092`)       |
| `topic_prefix`     | Prefix to strip from topic names for table names         |
| `subscribe_pattern`| Kafka topic subscription pattern (default: `{prefix}.*`)|
| `starting_offsets` | Where to start reading (default: `earliest`)             |
| `fail_on_data_loss`| Fail on data loss (default: `false`)                     |

### Processing

```python
{
    "error_strategy": "SKIP_ON_ERROR",
    "checkpoint_location": "./checkpoints/kafka_to_postgres",
}
```



## Error Handling

### `RAISE`

Fail the current batch immediately when an error occurs.

```python
"error_strategy": "RAISE"
```

### `SKIP_ON_ERROR`

Skip invalid records and continue processing the remaining batch.

```python
"error_strategy": "SKIP_ON_ERROR"
```



## API Reference

### `Stream2Pg`

```python
Stream2Pg(config, on_metrics=None)
```

Creates a streaming sink instance.

Parameters:

| Parameter    | Description               |
|  | - |
| `config`     | Configuration dictionary  |
| `on_metrics` | Optional metrics callback |



### `Stream2Pg.run()`

Starts the Kafka → PostgreSQL streaming pipeline.

```python
sink.run()
```



### `run()`

Convenience wrapper around `Stream2Pg`.

```python
from stream2pg import run

run(config)
```



### Exceptions

#### `ConfigurationError`

Raised when the provided configuration is invalid.

```python
from stream2pg.errors import ConfigurationError
```



## Requirements

* Python 3.9+
* Apache Spark
* Kafka
* PostgreSQL



## License

[MIT](./LICENSE)
