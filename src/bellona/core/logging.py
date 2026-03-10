import logging
import sys

import structlog


def setup_logging(
    level: str = "INFO",
    json_output: bool = False,
) -> None:
    """
    Configure structlog + stdlib logging for the entire application.

    Args:
        level: Root log level. Use "DEBUG" in dev for per-record output.
        json_output: If True, emit JSON lines (for production / log aggregators).
                     If False, emit human-readable colored console output.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Shared processors run on every log event regardless of output format.
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if json_output:
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    # Configure structlog to use stdlib under the hood so that logs from
    # libraries (SQLAlchemy, httpx, uvicorn) are captured and formatted
    # through the same pipeline.
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Wire stdlib logging to use structlog's formatter so third-party
    # library logs (SQLAlchemy, httpx, uvicorn) look the same.
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level)

    # Quiet down noisy libraries unless we're at DEBUG.
    for noisy in ("httpx", "httpcore", "sqlalchemy.engine"):
        logging.getLogger(noisy).setLevel(max(log_level, logging.WARNING))


def bind_job_context(job_id: str, connector_id: str | None = None) -> structlog.stdlib.BoundLogger:
    """
    Convenience for ingestion jobs: returns a logger with job_id and
    connector_id pre-bound so every subsequent log line includes them.

    Usage:
        log = bind_job_context(str(job.id), str(connector.id))
        log.info("started")
        log.warning("validation failed", field="age", error="not an integer")
    """
    log = structlog.get_logger()
    log = log.bind(job_id=job_id)
    if connector_id:
        log = log.bind(connector_id=connector_id)
    return log
