from __future__ import annotations
from typing import Any, Callable, Optional

from .config import ErrorStrategy, from_config
from .core import create_kafka_stream, create_spark_session, process_batch


class Stream2Pg:
    def __init__(
        self,
        config: dict[str, Any],
        on_metrics: Optional[Callable[..., None]] = None,
    ):
        self.config = from_config(config, on_metrics=on_metrics)
        self.on_metrics = on_metrics

    def run(self) -> None:
        cfg = self.config

        postgres_cfg = cfg["postgres"]
        kafka_cfg = cfg["kafka"]
        processing_cfg = cfg["processing"]

        error_strategy_str = processing_cfg.get("error_strategy", "raise")
        error_strategy = ErrorStrategy(error_strategy_str)

        checkpoint_location = processing_cfg.get(
            "checkpoint_location", "./checkpoints/kafka_to_postgres"
        )

        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        df = create_kafka_stream(spark, kafka_cfg)

        topic_prefix = kafka_cfg.get("topic_prefix", "")

        def foreach_batch_fn(batch_df, batch_id):
            process_batch(
                batch_df,
                batch_id,
                postgres_cfg,
                error_strategy,
                topic_prefix,
                on_metrics=self.on_metrics,
            )

        query = (
            df.writeStream.foreachBatch(foreach_batch_fn)
            .option("checkpointLocation", checkpoint_location)
            .start()
        )
        query.awaitTermination()


def run(
    config: dict[str, Any],
    on_metrics: Optional[Callable[..., None]] = None,
) -> None:
    sink = Stream2Pg(config, on_metrics=on_metrics)
    sink.run()
