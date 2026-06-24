from __future__ import annotations
from enum import Enum
from typing import Any, Callable, Optional

from .errors import ConfigurationError


class ErrorStrategy(Enum):
    RAISE = "raise"
    SKIP_ON_ERROR = "skip_on_error"


def from_config(
    config: dict[str, Any],
    on_metrics: Optional[Callable[..., None]] = None,
) -> dict[str, Any]:
    missing = []
    for key in ("postgres", "kafka", "processing"):
        if key not in config:
            missing.append(key)

    if missing:
        raise ConfigurationError(f"Missing required config keys: {', '.join(missing)}")

    return {
        "postgres": config["postgres"],
        "kafka": config["kafka"],
        "processing": config["processing"],
        "on_metrics": on_metrics,
    }
