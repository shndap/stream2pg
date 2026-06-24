from __future__ import annotations
import json
import time
from typing import Any, Callable, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

from .config import ErrorStrategy
from .db import ensure_columns, ensure_table, insert_record


def create_spark_session(app_name: str = "KafkaToPostgres") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2",
        )
        .getOrCreate()
    )


def process_batch(
    batch_df: Any,
    batch_id: int,
    pg_config: dict[str, Any],
    error_strategy: ErrorStrategy,
    on_metrics: Optional[Callable[..., None]] = None,
) -> None:
    start_time = time.time()
    row_count = 0
    inserted_count = 0
    skipped_count = 0

    if batch_df.isEmpty():
        return

    row_count = batch_df.count()
    conn = pg_config.get("connection")
    if conn is None:
        import psycopg2

        conn = psycopg2.connect(
            **{k: v for k, v in pg_config.items() if k != "connection"}
        )
        owns_connection = True
    else:
        owns_connection = False

    try:
        known_columns: dict[str, frozenset] = {}
        for row in batch_df.toLocalIterator():
            table_name = row.topic.replace("mobility-", "")
            try:
                record = json.loads(row.value)
            except json.JSONDecodeError:
                skipped_count += 1
                if error_strategy == ErrorStrategy.RAISE:
                    raise
                continue

            ensure_table(conn, table_name)
            current_schema = frozenset(record.keys())
            if known_columns.get(table_name) != current_schema:
                ensure_columns(conn, table_name, record)
                known_columns[table_name] = current_schema

            try:
                insert_record(conn, table_name, record)
                inserted_count += 1
            except Exception:
                if error_strategy == ErrorStrategy.RAISE:
                    raise
                skipped_count += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if owns_connection:
            conn.close()

    elapsed_ms = int((time.time() - start_time) * 1000)
    if on_metrics:
        on_metrics(batch_id, row_count, inserted_count, skipped_count, elapsed_ms)


def create_kafka_stream(
    spark: SparkSession,
    kafka_config: dict[str, Any],
    schema: Optional[StructType] = None,
):
    reader = (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            kafka_config.get("bootstrap_servers", "localhost:9092"),
        )
        .option(
            "subscribePattern", kafka_config.get("subscribe_pattern", "mobility-.*")
        )
        .option("startingOffsets", kafka_config.get("starting_offsets", "earliest"))
        .option(
            "failOnDataLoss",
            str(kafka_config.get("fail_on_data_loss", "false")).lower(),
        )
    )

    df = reader.load()
    df = df.select(
        col("topic"),
        col("value").cast("string").alias("value"),
    )
    return df
