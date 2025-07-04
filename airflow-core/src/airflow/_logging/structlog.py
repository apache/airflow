#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import logging
import os
import warnings
from functools import cache
from typing import Any, BinaryIO, Callable, Generic, Protocol, TextIO, TypeVar, Union, cast

import pygtrie
import structlog
from structlog.processors import NAME_TO_LEVEL
from structlog.typing import (
    BindableLogger,
    EventDict,
    FilteringBoundLogger,
    WrappedLogger,
)

log = logging.getLogger(__name__)


class AirflowFilteringBoundLogger(FilteringBoundLogger, Protocol):
    def isEnabledFor(self, level: int): ...
    def getEffectiveLevel(self) -> int: ...

    name: str


LEVEL_TO_FILTERING_LOGGER: dict[int, type[AirflowFilteringBoundLogger]] = {}


def _make_airflow_structlogger(min_level):
    # This uses https://github.com/hynek/structlog/blob/2f0cc42d/src/structlog/_native.py#L126
    # as inspiration

    LEVEL_TO_NAME = {v: k for k, v in NAME_TO_LEVEL.items()}

    def isEnabledFor(self: Any, level):
        return self.is_enabled_for(level)

    def getEffectiveLevel(self: Any):
        return self.get_effective_level()

    @property
    def name(self):
        return self._logger.name

    base = structlog.make_filtering_bound_logger(min_level)

    cls = type(
        f"AirflowBoundLoggerFilteringAt{LEVEL_TO_NAME.get(min_level, 'Notset').capitalize()}",
        (base,),
        {
            "isEnabledFor": isEnabledFor,
            "getEffectiveLevel": getEffectiveLevel,
            "name": name,
        },
    )
    LEVEL_TO_FILTERING_LOGGER[min_level] = cls
    return cls


AirflowBoundLoggerFilteringAtNotset = _make_airflow_structlogger(NAME_TO_LEVEL["notset"])
AirflowBoundLoggerFilteringAtDebug = _make_airflow_structlogger(NAME_TO_LEVEL["debug"])
AirflowBoundLoggerFilteringAtInfo = _make_airflow_structlogger(NAME_TO_LEVEL["info"])
AirflowBoundLoggerFilteringAtWarning = _make_airflow_structlogger(NAME_TO_LEVEL["warning"])
AirflowBoundLoggerFilteringAtError = _make_airflow_structlogger(NAME_TO_LEVEL["error"])
AirflowBoundLoggerFilteringAtCritical = _make_airflow_structlogger(NAME_TO_LEVEL["critical"])

# We use a trie structure (sometimes also called a "prefix tree") so that we can easily and quickly find the
# most suitable log level to apply. This mirrors the logging level cascade behavior from stdlib logging,
# without the complexity of multiple handlers etc
PER_LOGGER_LEVELS = pygtrie.StringTrie(separator=".")
PER_LOGGER_LEVELS.update(
    {
        # Top level logging default - changed to respect config in `configure_structlog`
        "": NAME_TO_LEVEL["info"],
    }
)


def make_filtering_logger() -> Callable[..., BindableLogger]:
    def maker(logger: WrappedLogger, *args, **kwargs):
        # If the logger is a NamedBytesLogger/NamedWriteLogger (an Airflow specific subclass) then
        # look up the global per-logger config and redirect to a new class.

        logger_name = kwargs.get("context", {}).get("logger_name")
        if not logger_name and isinstance(logger, (NamedWriteLogger, NamedBytesLogger)):
            logger_name = logger.name

        if logger_name:
            level = PER_LOGGER_LEVELS.longest_prefix(logger_name).get(PER_LOGGER_LEVELS[""])
        else:
            level = PER_LOGGER_LEVELS[""]
        return LEVEL_TO_FILTERING_LOGGER[level](logger, *args, **kwargs)  # type: ignore[call-arg]

    return maker


class NamedBytesLogger(structlog.BytesLogger):
    __slots__ = ("name",)

    def __init__(self, name: str | None = None, file: BinaryIO | None = None):
        self.name = name
        super().__init__(file)


class NamedWriteLogger(structlog.WriteLogger):
    __slots__ = ("name",)

    def __init__(self, name: str | None = None, file: TextIO | None = None):
        self.name = name
        super().__init__(file)


LogOutputType = TypeVar("LogOutputType", bound=Union[TextIO, BinaryIO])


class LoggerFactory(Generic[LogOutputType]):
    def __init__(
        self,
        cls: Callable[[str | None, LogOutputType | None], WrappedLogger],
        io: LogOutputType | None = None,
    ):
        self.cls = cls
        self.io = io

    def __call__(self, logger_name: str | None = None, *args: Any) -> WrappedLogger:
        return self.cls(logger_name, self.io)


def logger_name(logger: Any, method_name: Any, event_dict: EventDict) -> EventDict:
    if logger_name := (event_dict.pop("logger_name", None) or getattr(logger, "name", None)):
        event_dict.setdefault("logger", logger_name)
    return event_dict


# `eyJ` is `{"` in base64 encoding -- and any value that starts like that is in high likely hood a JWT
# token. Better safe than sorry
def redact_jwt(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    for k, v in event_dict.items():
        if isinstance(v, str) and v.startswith("eyJ"):
            event_dict[k] = "eyJ***"
    return event_dict


def drop_positional_args(logger: Any, method_name: Any, event_dict: EventDict) -> EventDict:
    event_dict.pop("positional_args", None)
    return event_dict


# This is a placeholder fn, that is "edited" in place via the `suppress_logs_and_warning` decorator
# The reason we need to do it this way is that structlog caches loggers on first use, and those include the
# configured processors, so we can't get away with changing the config as it won't have any effect once the
# logger obj is created and has been used once
def respect_stdlib_disable(logger: Any, method_name: Any, event_dict: EventDict) -> EventDict:
    return event_dict


@cache
def structlog_processors(
    json_output: bool,
):
    if json_output:
        timestamper = structlog.processors.MaybeTimeStamper(fmt="iso")
    else:
        timestamper = structlog.processors.MaybeTimeStamper(fmt="%Y-%m-%d %H:%M:%S.%f")

    processors: list[structlog.typing.Processor] = [
        respect_stdlib_disable,
        timestamper,
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        logger_name,
        redact_jwt,
        structlog.processors.StackInfoRenderer(),
    ]

    # Imports to suppress showing code from these modules. We need the import to get the filepath for
    # structlog to ignore.
    import contextlib

    import click
    import httpcore
    import httpx

    suppress = (
        click,
        contextlib,
        httpx,
        httpcore,
        httpx,
    )

    if json_output:
        dict_exc_formatter = structlog.tracebacks.ExceptionDictTransformer(
            use_rich=False, show_locals=False, suppress=suppress
        )

        dict_tracebacks = structlog.processors.ExceptionRenderer(dict_exc_formatter)

        import msgspec

        def json_dumps(msg, default):
            # Note: this is likely an "expensive" step, but lets massage the dict order for nice
            # viewing of the raw JSON logs.
            # Maybe we don't need this once the UI renders the JSON instead of displaying the raw text
            msg = {
                "timestamp": msg.pop("timestamp"),
                "level": msg.pop("level"),
                "event": msg.pop("event"),
                **msg,
            }
            return msgspec.json.encode(msg, enc_hook=default)

        def json_processor(logger: Any, method_name: Any, event_dict: EventDict) -> str:
            # Stdlib logging doesn't need the re-ordering, it's fine as it is
            return msgspec.json.encode(event_dict).decode("utf-8")

        json = structlog.processors.JSONRenderer(serializer=json_dumps)

        processors.extend(
            (
                dict_tracebacks,
                structlog.processors.UnicodeDecoder(),
                json,
            ),
        )

        return processors, {
            "timestamper": timestamper,
            "dict_tracebacks": dict_tracebacks,
            "json": json_processor,
        }
    rich_exc_formatter = structlog.dev.RichTracebackFormatter(
        # These values are picked somewhat arbitrarily to produce useful-but-compact tracebacks. If
        # we ever need to change these then they should be configurable.
        extra_lines=0,
        max_frames=30,
        indent_guides=False,
        suppress=suppress,
    )
    my_styles = structlog.dev.ConsoleRenderer.get_default_level_styles()
    my_styles["debug"] = structlog.dev.CYAN

    console = structlog.dev.ConsoleRenderer(exception_formatter=rich_exc_formatter, level_styles=my_styles)
    processors.append(console)
    return processors, {
        "timestamper": timestamper,
        "console": console,
    }


def configure_structlog(
    json_output: bool = False,
    log_level: str = "DEBUG",
    cache_logger_on_first_use: bool = True,
    stdlib_config: dict | None = None,
):
    if "fatal" not in NAME_TO_LEVEL:
        NAME_TO_LEVEL["fatal"] = NAME_TO_LEVEL["critical"]

    stdlib_config = stdlib_config or {}

    """Set up struct logging and stdlib logging config."""
    PER_LOGGER_LEVELS[""] = NAME_TO_LEVEL[log_level.lower()]

    if json_output:
        formatter = "plain"
    else:
        formatter = "colored"
    processors, named = structlog_processors(json_output)
    timestamper = named["timestamper"]

    pre_chain: list[structlog.typing.Processor] = [
        # Add the log level and a timestamp to the event_dict if the log entry
        # is not from structlog.
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        timestamper,
        structlog.contextvars.merge_contextvars,
        redact_jwt,
    ]

    # Don't cache the loggers during tests, it make it hard to capture them
    if "PYTEST_VERSION" in os.environ:
        cache_logger_on_first_use = False

    color_formatter: list[structlog.typing.Processor] = [
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
        drop_positional_args,
    ]
    std_lib_formatter: list[structlog.typing.Processor] = [
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
        drop_positional_args,
    ]

    wrapper_class = cast("type[BindableLogger]", make_filtering_logger())
    if json_output:
        structlog.configure(
            processors=processors,
            cache_logger_on_first_use=cache_logger_on_first_use,
            wrapper_class=wrapper_class,
            logger_factory=LoggerFactory(NamedBytesLogger),  # type: ignore[type-var]
        )

        pre_chain.append(named["dict_tracebacks"])
        color_formatter.append(named["json"])
        std_lib_formatter.append(named["json"])
    else:
        structlog.configure(
            processors=processors,
            cache_logger_on_first_use=cache_logger_on_first_use,
            wrapper_class=wrapper_class,
            logger_factory=LoggerFactory(NamedWriteLogger),  # type: ignore[type-var]
        )
        color_formatter.append(named["console"])

    global _warnings_showwarning

    if _warnings_showwarning is None:
        _warnings_showwarning = warnings.showwarning
        # Capture warnings and show them via structlog
        warnings.showwarning = _showwarning

    import logging.config

    config = {**stdlib_config}
    config["formatters"] = {**config["formatters"]}
    config["handlers"] = {**config["handlers"]}
    config["loggers"] = {**config["loggers"]}
    config["formatters"].update(
        {
            "plain": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processors": std_lib_formatter,
                "foreign_pre_chain": pre_chain,
                "pass_foreign_args": True,
            },
            "colored": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processors": color_formatter,
                "foreign_pre_chain": pre_chain,
                "pass_foreign_args": True,
            },
        }
    )
    for section in (config["loggers"], config["handlers"]):
        for log_config in section.values():
            # We want everything to go via structlog, remove whatever the user might have configured
            log_config.pop("stream", None)
            log_config.pop("formatter", None)
            # log_config.pop("handlers", None)

    config["handlers"].update(
        {
            "default": {
                "level": log_level.upper(),
                "class": "logging.StreamHandler",
                "formatter": formatter,
            },
        }
    )
    config["loggers"].update(
        {
            # Set Airflow logging to the level requested, but most everything else at "INFO"
            "airflow": {"level": log_level.upper()},
            # These ones are too chatty even at info
            "httpx": {"level": "WARN"},
            "sqlalchemy.engine": {"level": "WARN"},
        }
    )
    config["root"] = {
        "handlers": ["default"],
        "level": "INFO",
        "propagate": True,
    }

    logging.config.dictConfig(config)


_warnings_showwarning: Any = None


def _showwarning(
    message: Warning | str,
    category: type[Warning],
    filename: str,
    lineno: int,
    file: TextIO | None = None,
    line: str | None = None,
) -> Any:
    """
    Redirects warnings to structlog so they appear in task logs etc.

    Implementation of showwarnings which redirects to logging, which will first
    check to see if the file parameter is None. If a file is specified, it will
    delegate to the original warnings implementation of showwarning. Otherwise,
    it will call warnings.formatwarning and will log the resulting string to a
    warnings logger named "py.warnings" with level logging.WARNING.
    """
    if file is not None:
        if _warnings_showwarning is not None:
            _warnings_showwarning(message, category, filename, lineno, file, line)
    else:
        log = structlog.get_logger("py.warnings")
        log.warning(str(message), category=category.__name__, filename=filename, lineno=lineno)
