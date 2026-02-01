"""Logging configuration for jestr."""

import logging.config
from contextlib import contextmanager

import orjson
import structlog


@contextmanager
def logging_context(batch_id: str, source_name: str, resource_name: str):
    """Context manager to add contextual information to logs."""
    for key in ("batch_id", "source_name", "resource_name"):
        structlog.contextvars.unbind_contextvars(key)

    try:
        for key, value in locals().items():
            if value is not None:
                structlog.contextvars.bind_contextvars(**{key: value})
        yield
    finally:
        structlog.contextvars.clear_contextvars()


def _orjson_serialiser(obj, **kwargs) -> str:
    """Serialize object to JSON, with fallback for non-serializable types."""
    try:
        return orjson.dumps(obj, default=str).decode("utf-8")
    except (TypeError, ValueError) as exc:
        return orjson.dumps(
            {
                "error": "Serialisation failed",
                "type": str(obj),
                "exception": str(exc),
            }
        ).decode("utf-8")


def _get_renderer(json_output: bool):
    """Get the appropriate renderer based on output format."""
    if json_output:
        return structlog.processors.JSONRenderer(serializer=_orjson_serialiser)
    return structlog.dev.ConsoleRenderer(colors=True)


def _build_handlers_config(console_output: bool) -> tuple[dict, list]:
    """Build handlers configuration and root handlers list."""
    handlers_config = {}
    root_handlers = []

    if console_output:
        handlers_config["console"] = {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
            "formatter": "structured",
        }
        root_handlers.append("console")

    # Write logs to file, overwriting on each run
    handlers_config["file"] = {
        "class": "logging.FileHandler",
        "filename": "jestr.log",
        "mode": "w",
        "formatter": "file_structured",
    }
    root_handlers.append("file")

    return handlers_config, root_handlers


def _cleanup_logging() -> None:
    """Clear and reset all existing loggers and handlers."""
    # Clear any handlers from root logger
    for handler in logging.root.handlers[:]:
        if hasattr(handler, "close"):
            handler.close()
        logging.root.removeHandler(handler)

    # Reset all existing loggers to avoid configuration conflicts
    for logger_name in list(logging.root.manager.loggerDict.keys()):
        existing_logger = logging.getLogger(logger_name)
        for handler in existing_logger.handlers[:]:
            if hasattr(handler, "close"):
                handler.close()
            existing_logger.removeHandler(handler)


def setup_logger(
    level: str = "INFO",
    *,
    json_output: bool = False,
    console_output: bool = True,
    cache_logger_on_first_use: bool = True,
) -> None:
    """Initialise structured logging."""
    # Processors shared between structlog and standard logging
    pre_chain = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.TimeStamper(fmt="iso"),
        structlog.stdlib.ExtraAdder(),
    ]

    # Production processors for enhanced debugging and safety
    production_processors = [
        structlog.processors.UnicodeDecoder(),  # Ensure all strings are unicode
        structlog.processors.CallsiteParameterAdder(
            {
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
            }
        ),
    ]

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.contextvars.merge_contextvars,
            *pre_chain,
            *production_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=cache_logger_on_first_use,
    )

    # Configure standard logging with ProcessorFormatter
    handlers_config, root_handlers = _build_handlers_config(console_output)
    _cleanup_logging()
    renderer = _get_renderer(json_output)

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {
                "structured": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processors": [
                        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                        renderer,
                    ],
                    "foreign_pre_chain": pre_chain,
                },
                "file_structured": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processors": [
                        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                        structlog.processors.KeyValueRenderer(key_order=None),
                    ],
                    "foreign_pre_chain": pre_chain,
                },
            },
            "handlers": handlers_config,
            "root": {
                "level": level,
                "handlers": root_handlers,
            },
        }
    )
