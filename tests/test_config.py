from __future__ import annotations

import pytest
from stream2pg.config import ErrorStrategy, from_config
from stream2pg.errors import ConfigurationError


class TestFromConfig:
    def test_raises_on_missing_postgres_key(self):
        config = {
            "kafka": {"bootstrap_servers": "localhost:9092"},
            "processing": {"error_strategy": "raise"},
        }
        with pytest.raises(ConfigurationError) as exc_info:
            from_config(config)
        assert "postgres" in str(exc_info.value)

    def test_raises_on_missing_kafka_key(self):
        config = {
            "postgres": {"host": "localhost"},
            "processing": {"error_strategy": "raise"},
        }
        with pytest.raises(ConfigurationError) as exc_info:
            from_config(config)
        assert "kafka" in str(exc_info.value)

    def test_raises_on_missing_processing_key(self):
        config = {
            "postgres": {"host": "localhost"},
            "kafka": {"bootstrap_servers": "localhost:9092"},
        }
        with pytest.raises(ConfigurationError) as exc_info:
            from_config(config)
        assert "processing" in str(exc_info.value)

    def test_returns_config_with_on_metrics(self):
        config = {
            "postgres": {"host": "localhost"},
            "kafka": {"bootstrap_servers": "localhost:9092"},
            "processing": {"error_strategy": "raise"},
        }
        on_metrics = lambda *args: None
        result = from_config(config, on_metrics=on_metrics)
        assert result["postgres"] == config["postgres"]
        assert result["kafka"] == config["kafka"]
        assert result["processing"] == config["processing"]
        assert result["on_metrics"] == on_metrics


class TestErrorStrategy:
    def test_raise_value(self):
        assert ErrorStrategy.RAISE.value == "raise"

    def test_skip_on_error_value(self):
        assert ErrorStrategy.SKIP_ON_ERROR.value == "skip_on_error"

    def test_from_str(self):
        assert ErrorStrategy("raise") == ErrorStrategy.RAISE
        assert ErrorStrategy("skip_on_error") == ErrorStrategy.SKIP_ON_ERROR
