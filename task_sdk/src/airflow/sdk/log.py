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

import io
import itertools
import logging.config
import os
import sys
import warnings
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, Callable, Generic, TextIO, TypeVar

import msgspec
import structlog

if TYPE_CHECKING:
    from structlog.typing import EventDict, ExcInfo, Processor


__all__ = [
    "configure_logging",
    "reset_logging",
]


def exception_group_tracebacks(
    format_exception: Callable[[ExcInfo], list[dict[str, Any]]],
) -> Processor:
    # Make mypy happy
    if not hasattr(__builtins__, "BaseExceptionGroup"):
        T = TypeVar("T")

        class BaseExceptionGroup(Generic[T]):
            exceptions: list[T]

    def _exception_group_tracebacks(logger: Any, method_name: Any, event_dict: EventDict) -> EventDict:
        if exc_info := event_dict.get("exc_info", None):
            group: BaseExceptionGroup[Exception] | None = None
            if exc_info is True:
                # `log.exception('mesg")` case
                exc_info = sys.exc_info()
                if exc_info[0] is None:
                    exc_info = None

            if (
                isinstance(exc_info, tuple)
                and len(exc_info) == 3
                and isinstance(exc_info[1], BaseExceptionGroup)
            ):
                group = exc_info[1]
            elif isinstance(exc_info, BaseExceptionGroup):
                group = exc_info

            if group:
                # Only remove it from event_dict if we handle it
                del event_dict["exc_info"]
                event_dict["exception"] = list(
                    itertools.chain.from_iterable(
                        format_exception((type(exc), exc, exc.__traceback__))  # type: ignore[attr-defined,arg-type]
                        for exc in (*group.exceptions, group)
                    )
                )

        return event_dict

    return _exception_group_tracebacks


def logger_name(logger: Any, method_name: Any, event_dict: EventDict) -> EventDict:
    if logger_name := event_dict.pop("logger_name", None):
        event_dict.setdefault("logger", logger_name)
    return event_dict


def redact_jwt(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    for k, v in event_dict.items():
        if isinstance(v, str) and v.startswith("eyJ"):
            event_dict[k] = "eyJ***"
    return event_dict


def drop_positional_args(logger: Any, method_name: Any, event_dict: EventDict) -> EventDict:
    event_dict.pop("positional_args", None)
    return event_dict


class StdBinaryStreamHandler(logging.StreamHandler):
    """A logging.StreamHandler that sends logs as binary JSON over the given stream."""

    stream: BinaryIO

    def __init__(self, stream: BinaryIO):
        super().__init__(stream)

    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            buffer = bytearray(msg, "utf-8", "backslashreplace")

            buffer += b"\n"

            stream = self.stream
            stream.write(buffer)
            self.flush()
        except RecursionError:  # See issue 36272
            raise
        except Exception:
            self.handleError(record)


def _common_processors(timestamp_fmt):
    timestamper = structlog.processors.MaybeTimeStamper(fmt=timestamp_fmt)
    processors: list[structlog.typing.Processor] = [
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
    return timestamper, processors, suppress


@cache
def _logging_processors_pretty():
    timestamper, processors, suppress = _common_processors(timestamp_fmt="%Y-%m-%d %H:%M:%S.%f")

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

    console_renderer = structlog.dev.ConsoleRenderer(
        exception_formatter=rich_exc_formatter, level_styles=my_styles
    )
    processors.append(console_renderer)
    return processors, timestamper, console_renderer


@cache
def _logging_processors_plain():
    timestamper, processors, suppress = _common_processors(timestamp_fmt="iso")
    dict_exc_formatter = structlog.tracebacks.ExceptionDictTransformer(
        use_rich=False, show_locals=False, suppress=suppress
    )

    dict_tracebacks = structlog.processors.ExceptionRenderer(dict_exc_formatter)
    if hasattr(__builtins__, "BaseExceptionGroup"):
        exc_group_processor = exception_group_tracebacks(dict_exc_formatter)
        processors.append(exc_group_processor)
    else:
        exc_group_processor = None

    def json_dumps(msg, default):
        # Note: this is likely an "expensive" step, but lets massage the dict order for nice
        # viewing of the raw JSON logs.
        # Maybe we don't need this once the UI renders the JSON instead of displaying the raw text
        # msg = {
        #     "timestamp": msg.pop("timestamp"),
        #     "level": msg.pop("level"),
        #     "event": msg.pop("event"),
        #     **msg,
        # }
        return msgspec.json.encode(msg, enc_hook=default)

    json = structlog.processors.JSONRenderer(serializer=json_dumps)

    processors.extend(
        (
            dict_tracebacks,
            structlog.processors.UnicodeDecoder(),
            json,
        ),
    )

    return processors, timestamper, exc_group_processor, dict_tracebacks


def _json_processor(logger: Any, method_name: Any, event_dict: EventDict) -> str:
    # Stdlib logging doesn't need the re-ordering, it's fine as it is
    return msgspec.json.encode(event_dict).decode("utf-8")


@cache
def configure_logging(
    enable_pretty_log: bool = True,
    log_level: str = "INFO",
    output: BinaryIO | TextIO | None = None,
    cache_logger_on_first_use: bool = True,
    sending_to_supervisor: bool = False,
):
    """Set up struct logging and stdlib logging config."""
    # Don't cache the loggers during tests, it make it hard to capture them
    if "PYTEST_CURRENT_TEST" in os.environ:
        cache_logger_on_first_use = False

    color_format_processors: list[structlog.typing.Processor] = [
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
        drop_positional_args,
    ]
    plain_format_processors: list[structlog.typing.Processor] = [
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
        drop_positional_args,
    ]
    if enable_pretty_log:
        formatter = "colored"
        logger_factory, pre_chain_add, processors, timestamper, console_renderer = _configure_logging_pretty(
            output=output,
        )
        color_format_processors.append(console_renderer)
    else:
        formatter = "plain"
        color_format_processors.append(_json_processor)
        plain_format_processors.append(_json_processor)  # why do we conditionally add json processor here?
        logger_factory, output, pre_chain_add, processors, timestamper = _configure_logging_plain(
            output=output,
        )

    pre_chain: list[structlog.typing.Processor] = [
        # Add the log level and a timestamp to the event_dict if the log entry
        # is not from structlog.
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        timestamper,
        structlog.contextvars.merge_contextvars,
        redact_jwt,
        *pre_chain_add,
    ]

    structlog.configure(
        processors=processors,
        cache_logger_on_first_use=cache_logger_on_first_use,
        wrapper_class=structlog.make_filtering_bound_logger(log_level.lower()),
        logger_factory=logger_factory,
    )
    global _warnings_showwarning

    if _warnings_showwarning is None:
        _warnings_showwarning = warnings.showwarning
        # Capture warnings and show them via structlog
        warnings.showwarning = _showwarning

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "plain": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processors": plain_format_processors,
                    "foreign_pre_chain": pre_chain,
                    "pass_foreign_args": True,
                },
                "colored": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processors": color_format_processors,
                    "foreign_pre_chain": pre_chain,
                    "pass_foreign_args": True,
                },
            },
            "handlers": {
                "default": {
                    "level": log_level.upper(),
                    "class": "logging.StreamHandler",
                    "formatter": formatter,
                },
                "to_supervisor": {
                    "level": log_level.upper(),
                    "()": StdBinaryStreamHandler,
                    "formatter": formatter,
                    "stream": output,
                },
            },
            "loggers": {
                # Set Airflow logging to the level requested, but most everything else at "INFO"
                "": {
                    "handlers": ["to_supervisor" if sending_to_supervisor else "default"],
                    "level": "INFO",
                    "propagate": True,
                },
                "airflow": {"level": log_level.upper()},
                # These ones are too chatty even at info
                "httpx": {"level": "WARN"},
                "sqlalchemy.engine": {"level": "WARN"},
            },
        }
    )


def _configure_logging_plain(*, output):
    processors, timestamper, exc_group_processor, dict_tracebacks = _logging_processors_plain()
    if output is not None and "b" not in output.mode:
        if not hasattr(output, "buffer"):
            raise ValueError(
                f"output needed to be a binary stream, but it didn't have a buffer attribute ({output=})"
            )
        else:
            output = output.buffer
    if TYPE_CHECKING:
        # Not all binary streams are isinstance of BinaryIO, so we check via looking at `mode` at
        # runtime. mypy doesn't grok that though
        assert isinstance(output, BinaryIO)
    logger_factory = structlog.BytesLoggerFactory(output)
    pre_chain_add = []
    if exc_group_processor:
        pre_chain_add.append(exc_group_processor)
    pre_chain_add.append(dict_tracebacks)
    return logger_factory, output, pre_chain_add, processors, timestamper


def _configure_logging_pretty(*, output):
    if output is not None and not isinstance(output, TextIO):
        wrapper = io.TextIOWrapper(output, line_buffering=True)
        logger_factory = structlog.WriteLoggerFactory(wrapper)
    else:
        logger_factory = structlog.WriteLoggerFactory(output)
    processors, timestamper, console_renderer = _logging_processors_pretty()
    pre_chain_add = []
    return logger_factory, pre_chain_add, processors, timestamper, console_renderer


def reset_logging():
    global _warnings_showwarning
    warnings.showwarning = _warnings_showwarning
    configure_logging.cache_clear()


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
        log = structlog.get_logger(logger_name="py.warnings")
        log.warning(str(message), category=category.__name__, filename=filename, lineno=lineno)


def _prepare_log_folder(directory: Path, mode: int):
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
    for parent in reversed(directory.parents):
        parent.mkdir(mode=mode, exist_ok=True)
    directory.mkdir(mode=mode, exist_ok=True)


def init_log_file(local_relative_path: str) -> Path:
    """
    Ensure log file and parent directories are created.

    Any directories that are missing are created with the right permission bits.

    See above ``_prepare_log_folder`` method for more detailed explanation.
    """
    # NOTE: This is duplicated from airflow.utils.log.file_task_handler:FileTaskHandler._init_file, but we
    # want to remove that
    from airflow.configuration import conf

    new_file_permissions = int(
        conf.get("logging", "file_task_handler_new_file_permissions", fallback="0o664"),
        8,
    )
    new_folder_permissions = int(
        conf.get("logging", "file_task_handler_new_folder_permissions", fallback="0o775"),
        8,
    )

    base_log_folder = conf.get("logging", "base_log_folder")
    full_path = Path(base_log_folder, local_relative_path)

    _prepare_log_folder(full_path.parent, new_folder_permissions)

    try:
        full_path.touch(new_file_permissions)
    except OSError as e:
        log = structlog.get_logger()
        log.warning("OSError while changing ownership of the log file. %s", e)

    return full_path
