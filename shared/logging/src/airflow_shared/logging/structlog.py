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

import codecs
import io
import itertools
import logging
import os
import re
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from functools import cache, cached_property, partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, Generic, TextIO, TypeVar, cast

import pygtrie
import structlog
import structlog.processors
from structlog.processors import NAME_TO_LEVEL, CallsiteParameter

from ._noncaching import make_file_io_non_caching
from .percent_formatter import PercentFormatRender

if TYPE_CHECKING:
    from structlog.typing import (
        BindableLogger,
        EventDict,
        Processor,
        WrappedLogger,
    )

    from .types import Logger

log = logging.getLogger(__name__)

__all__ = [
    "configure_logging",
    "structlog_processors",
]

JWT_PATTERN = re.compile(r"eyJ[\.A-Za-z0-9-_]*")

LEVEL_TO_FILTERING_LOGGER: dict[int, type[Logger]] = {}


def _make_airflow_structlogger(min_level):
    # This uses https://github.com/hynek/structlog/blob/2f0cc42d/src/structlog/_native.py#L126
    # as inspiration

    LEVEL_TO_NAME = {v: k for k, v in NAME_TO_LEVEL.items()}

    # A few things, namely paramiko _really_ wants this to be a stdlib logger. These fns pretends it is enough
    # like it to function.
    @cached_property
    def handlers(self):
        return [logging.NullHandler()]

    @property
    def level(self):
        return min_level

    @property
    def name(self):
        return self._logger.name

    def _nop(self: Any, event: str, *args: Any, **kw: Any) -> Any:
        return None

    # Work around an issue in structlog https://github.com/hynek/structlog/issues/745
    def make_method(
        level: int,
    ) -> Callable[..., Any]:
        name = LEVEL_TO_NAME[level]
        if level < min_level:
            return _nop

        def meth(self: Any, event: str, *args: Any, **kw: Any) -> Any:
            if not args:
                return self._proxy_to_logger(name, event, **kw)

            # See for reason https://github.com/python/cpython/blob/3.13/Lib/logging/__init__.py#L307-L326
            if args and len(args) == 1 and isinstance(args[0], Mapping) and args[0]:
                args = args[0]
            return self._proxy_to_logger(name, event % args, **kw)

        meth.__name__ = name
        return meth

    base = structlog.make_filtering_bound_logger(min_level)

    cls = type(
        f"AirflowBoundLoggerFilteringAt{LEVEL_TO_NAME.get(min_level, 'Notset').capitalize()}",
        (base,),
        {
            "isEnabledFor": base.is_enabled_for,
            "getEffectiveLevel": base.get_effective_level,
            "level": level,
            "name": name,
            "handlers": handlers,
        }
        | {name: make_method(lvl) for lvl, name in LEVEL_TO_NAME.items()},
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
        # Top level logging default - changed to respect config in `configure_logging`
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
        if file is not None:
            file = make_file_io_non_caching(file)
        super().__init__(file)


class NamedWriteLogger(structlog.WriteLogger):
    __slots__ = ("name",)

    def __init__(self, name: str | None = None, file: TextIO | None = None):
        self.name = name
        if file is not None:
            file = make_file_io_non_caching(file)
        super().__init__(file)


LogOutputType = TypeVar("LogOutputType", bound=TextIO | BinaryIO)


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
        if isinstance(v, str):
            event_dict[k] = re.sub(JWT_PATTERN, "eyJ***", v)
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
    log_format: str = "",
    colors: bool = True,
    callsite_parameters: tuple[CallsiteParameter, ...] = (),
):
    """
    Create the correct list of structlog processors for the given config.

    Return value is a tuple of three elements:

    1. A list of processors shared for structlgo and stblib
    2. The final processor/renderer (one that outputs a string) for use with structlog.stdlib.ProcessorFormatter


    ``callsite_parameters`` specifies the keys to add to the log event dict. If ``log_format`` is specified
    then anything callsite related will be added to this list

    :meta private:
    """
    timestamper = structlog.processors.MaybeTimeStamper(fmt="iso")

    # Processors shared between stdlib handlers and structlog processors
    shared_processors: list[structlog.typing.Processor] = [
        respect_stdlib_disable,
        timestamper,
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        logger_name,
        redact_jwt,
        structlog.processors.StackInfoRenderer(),
    ]

    if log_format:
        # Maintain the order if any params that are given explicitly, then add on anything needed for the
        # format string (so use a dict with None as the values as set doesn't preserve order)
        params = {
            param: None
            for param in itertools.chain(
                callsite_parameters or [], PercentFormatRender.callsite_params_from_fmt_string(log_format)
            )
        }
        shared_processors.append(
            structlog.processors.CallsiteParameterAdder(list(params.keys()), additional_ignores=[__name__])
        )
    elif callsite_parameters:
        shared_processors.append(
            structlog.processors.CallsiteParameterAdder(callsite_parameters, additional_ignores=[__name__])
        )

    # Imports to suppress showing code from these modules. We need the import to get the filepath for
    # structlog to ignore.

    import contextlib

    import click

    suppress = (click, contextlib)
    try:
        import httpcore

        suppress += (httpcore,)
    except ImportError:
        pass
    try:
        import httpx

        suppress += (httpx,)
    except ImportError:
        pass

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

        json = structlog.processors.JSONRenderer(serializer=json_dumps)

        def json_processor(logger: Any, method_name: Any, event_dict: EventDict) -> str:
            return json(logger, method_name, event_dict).decode("utf-8")

        shared_processors.extend(
            (
                dict_tracebacks,
                structlog.processors.UnicodeDecoder(),
            ),
        )

        return shared_processors, json_processor, json

    if os.getenv("DEV", "") != "":
        # Only use Rich in dev -- optherwise for "production" deployments it makes the logs harder to read as
        # it uses lots of ANSI escapes and non ASCII characters. Simpler is better for non-dev non-JSON
        exc_formatter = structlog.dev.RichTracebackFormatter(
            # These values are picked somewhat arbitrarily to produce useful-but-compact tracebacks. If
            # we ever need to change these then they should be configurable.
            extra_lines=0,
            max_frames=30,
            indent_guides=False,
            suppress=suppress,
        )
    else:
        exc_formatter = structlog.dev.plain_traceback

    my_styles = structlog.dev.ConsoleRenderer.get_default_level_styles(colors=colors)
    if colors:
        my_styles["debug"] = structlog.dev.CYAN

    if log_format:
        console = PercentFormatRender(
            fmt=log_format,
            exception_formatter=exc_formatter,
            level_styles=my_styles,
            colors=colors,
        )
    else:
        if callsite_parameters == (CallsiteParameter.FILENAME, CallsiteParameter.LINENO):
            # Nicer formatting of the default callsite config
            def log_loc(logger: Any, method_name: Any, event_dict: EventDict) -> EventDict:
                if (
                    event_dict.get("logger") != "py.warnings"
                    and "filename" in event_dict
                    and "lineno" in event_dict
                ):
                    event_dict["loc"] = f"{event_dict.pop('filename')}:{event_dict.pop('lineno')}"
                return event_dict

            shared_processors.append(log_loc)
        console = structlog.dev.ConsoleRenderer(
            exception_formatter=exc_formatter,
            level_styles=my_styles,
            colors=colors,
        )

    return shared_processors, console, console


def configure_logging(
    *,
    json_output: bool = False,
    log_level: str = "DEBUG",
    log_format: str = "",
    stdlib_config: dict | None = None,
    extra_processors: Sequence[Processor] | None = None,
    callsite_parameters: Iterable[CallsiteParameter] | None = None,
    colors: bool = True,
    output: LogOutputType | None = None,
    log_levels: str | dict[str, str] | None = None,
    cache_logger_on_first_use: bool = True,
):
    """
    Configure structlog (and stbilb's logging to send via structlog processors too).

    If percent_log_format is passed then it will be handled in a similar mode to stdlib, including
    interpolations such as ``%(asctime)s`` etc.

    :param json_output: Set to true to write all logs as JSON (one per line)
    :param log_level: The default log level to use for most logs
    :param log_format: A percent-style log format to write non JSON logs with.
    :param output: Where to write the logs too. If ``json_output`` is true this must be a binary stream
    :param colors: Whether to use colors for non-JSON logs. This only works if standard out is a TTY (that is,
        an interactive session), unless overridden by environment variables described below.
        Please note that disabling colors also disables all styling, including bold and italics.
        The following environment variables control color behavior (set to any non-empty value to activate):

        * ``NO_COLOR`` - Disables colors completely. This takes precedence over all other settings,
        including ``FORCE_COLOR``.

        * ``FORCE_COLOR`` - Forces colors to be enabled, even when output is not going to a TTY. This only
        takes effect if ``NO_COLOR`` is not set.

    :param callsite_parameters: A list parameters about the callsite (line number, function name etc) to
        include in the logs.

        If ``log_format`` is specified, then anything required to populate that (such as ``%(lineno)d``) will
        be automatically included.
    :param log_levels: Levels of extra loggers to configure.

        To make this easier to use, this can be a string consisting of pairs of ``<logger>=<level>`` (either
        string, or space delimited) which will set the level for that specific logger.

        For example::

            ``sqlalchemy=INFO sqlalchemy.engine=DEBUG``
    """
    if "fatal" not in NAME_TO_LEVEL:
        NAME_TO_LEVEL["fatal"] = NAME_TO_LEVEL["critical"]

    def is_atty():
        return sys.stdout is not None and hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

    colors = os.environ.get("NO_COLOR", "") == "" and (
        os.environ.get("FORCE_COLOR", "") != "" or (colors and is_atty())
    )

    stdlib_config = stdlib_config or {}
    extra_processors = extra_processors or ()

    PER_LOGGER_LEVELS[""] = NAME_TO_LEVEL[log_level.lower()]

    # Extract per-logger-tree levels and set them
    if isinstance(log_levels, str):
        log_from_level = partial(re.compile(r"\s*=\s*").split, maxsplit=2)
        log_levels = {log: level for log, level in map(log_from_level, re.split(r"[\s,]+", log_levels))}
    if log_levels:
        for log, level in log_levels.items():
            try:
                loglevel = NAME_TO_LEVEL[level.lower()]
            except KeyError:
                raise ValueError(f"Invalid log level for logger {log!r}: {level!r}") from None
            else:
                PER_LOGGER_LEVELS[log] = loglevel

    shared_pre_chain, for_stdlib, for_structlog = structlog_processors(
        json_output,
        log_format=log_format,
        colors=colors,
        callsite_parameters=tuple(callsite_parameters or ()),
    )
    shared_pre_chain += list(extra_processors)
    pre_chain: list[structlog.typing.Processor] = [structlog.stdlib.add_logger_name] + shared_pre_chain

    # Don't cache the loggers during tests, it make it hard to capture them
    if "PYTEST_VERSION" in os.environ:
        cache_logger_on_first_use = False

    std_lib_formatter: list[Processor] = [
        # TODO: Don't include this if we are using PercentFormatter -- it'll delete something we
        # just have to recerated!
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
        drop_positional_args,
        for_stdlib,
    ]

    wrapper_class = cast("type[BindableLogger]", make_filtering_logger())
    if json_output:
        logger_factory = LoggerFactory(NamedBytesLogger, io=output)
    else:
        # There is no universal way of telling if a file-like-object is binary (and needs bytes) or text that
        # works for files, sockets and io.StringIO/BytesIO.

        # If given a binary object, wrap it in a text mode wrapper
        if output is not None and not hasattr(output, "encoding"):
            output = io.TextIOWrapper(output, line_buffering=True)
        logger_factory = LoggerFactory(NamedWriteLogger, io=output)

    structlog.configure(
        processors=shared_pre_chain + [for_structlog],
        cache_logger_on_first_use=cache_logger_on_first_use,
        wrapper_class=wrapper_class,
        logger_factory=logger_factory,
    )

    import logging.config

    config = {**stdlib_config}
    config.setdefault("version", 1)
    config.setdefault("disable_existing_loggers", False)
    config["formatters"] = {**config.get("formatters", {})}
    config["handlers"] = {**config.get("handlers", {})}
    config["loggers"] = {**config.get("loggers", {})}
    config["formatters"].update(
        {
            "structlog": {
                "()": structlog.stdlib.ProcessorFormatter,
                "use_get_message": False,
                "processors": std_lib_formatter,
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

    if output and not hasattr(output, "encoding"):
        # This is a BinaryIO, we need to give logging.StreamHandler a TextIO
        output = codecs.lookup("utf-8").streamwriter(output)  # type: ignore

    config["handlers"].update(
        {
            "default": {
                "level": log_level.upper(),
                "class": "logging.StreamHandler",
                "formatter": "structlog",
                "stream": output,
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


def init_log_folder(directory: str | os.PathLike[str], new_folder_permissions: int):
    """
    Prepare the log folder and ensure its mode is as configured.

    To handle log writing when tasks are impersonated, the log files need to
    be writable by the user that runs the Airflow command and the user
    that is impersonated. This is mainly to handle corner cases with the
    SubDagOperator. When the SubDagOperator is run, all of the operators
    run under the impersonated user and create appropriate log files
    as the impersonated user. However, if the user manually runs tasks
    of the SubDagOperator through the UI, then the log files are created
    by the user that runs the Airflow command. For example, the Airflow
    run command may be run by the `airflow_sudoable` user, but the Airflow
    tasks may be run by the `airflow` user. If the log files are not
    writable by both users, then it's possible that re-running a task
    via the UI (or vice versa) results in a permission error as the task
    tries to write to a log file created by the other user.

    We leave it up to the user to manage their permissions by exposing configuration for both
    new folders and new log files. Default is to make new log folders and files group-writeable
    to handle most common impersonation use cases. The requirement in this case will be to make
    sure that the same group is set as default group for both - impersonated user and main airflow
    user.
    """
    directory = Path(directory)
    for parent in reversed(Path(directory).parents):
        parent.mkdir(mode=new_folder_permissions, exist_ok=True)
    directory.mkdir(mode=new_folder_permissions, exist_ok=True)


def init_log_file(
    base_log_folder: str | os.PathLike[str],
    local_relative_path: str | os.PathLike[str],
    *,
    new_folder_permissions: int = 0o775,
    new_file_permissions: int = 0o664,
) -> Path:
    """
    Ensure log file and parent directories are created with the correct permissions.

    Any directories that are missing are created with the right permission bits.

    See above ``init_log_folder`` method for more detailed explanation.
    """
    full_path = Path(base_log_folder, local_relative_path)
    init_log_folder(full_path.parent, new_folder_permissions)

    try:
        full_path.touch(new_file_permissions)
    except OSError as e:
        log = structlog.get_logger(__name__)
        log.warning("OSError while changing ownership of the log file. %s", e)

    return full_path


def logger_without_processor_of_type(logger: WrappedLogger, processor_type: type):
    procs = getattr(logger, "_processors", None)
    if procs is None:
        procs = structlog.get_config()["processors"]
    procs = [proc for proc in procs if not isinstance(proc, processor_type)]

    return structlog.wrap_logger(
        getattr(logger, "_logger", None), processors=procs, **getattr(logger, "_context", {})
    )


if __name__ == "__main__":
    configure_logging(
        # json_output=True,
        log_format="[%(blue)s%(asctime)s%(reset)s] {%(blue)s%(filename)s:%(reset)s%(lineno)d} %(log_color)s%(levelname)s%(reset)s - %(log_color)s%(message)s%(reset)s",
    )
    log = logging.getLogger("testing.stlib")
    log2 = structlog.get_logger(logger_name="testing.structlog")

    def raises():
        try:
            1 / 0
        except ZeroDivisionError:
            log.exception("str")
        try:
            1 / 0
        except ZeroDivisionError:
            log2.exception("std")

    def main():
        log.info("in main")
        log2.info("in main", key="value")
        raises()

    main()
