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

import warnings
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, TextIO

import structlog
import structlog.processors

# We have to import this here, as it is used in the type annotations at runtime even if it seems it is
# not used in the code. This is because Pydantic uses type at runtime to validate the types of the fields.
from pydantic import JsonValue  # noqa: TC002

if TYPE_CHECKING:
    from structlog.typing import EventDict, FilteringBoundLogger, Processor

    from airflow.logging_config import RemoteLogIO
    from airflow.sdk.types import Logger, RuntimeTaskInstanceProtocol as RuntimeTI


__all__ = ["configure_logging", "reset_logging", "mask_secret"]


def mask_logs(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    from airflow.sdk._shared.secrets_masker import redact

    event_dict = redact(event_dict)  # type: ignore[assignment]
    return event_dict


@cache
def logging_processors(
    json_output: bool,
    log_format: str = "",
    colors: bool = True,
    sending_to_supervisor: bool = False,
) -> tuple[Processor, ...]:
    from airflow.sdk._shared.logging.structlog import structlog_processors

    extra_processors: tuple[Processor, ...] = ()

    mask_secrets = not sending_to_supervisor
    if mask_secrets:
        extra_processors += (mask_logs,)

    if (remote := load_remote_log_handler()) and (remote_processors := getattr(remote, "processors")):
        extra_processors += remote_processors

    procs, _, final_writer = structlog_processors(
        json_output=json_output, log_format=log_format, colors=colors
    )
    return tuple(procs) + extra_processors + (final_writer,)


@cache
def configure_logging(
    json_output: bool = False,
    log_level: str = "DEFAULT",
    output: BinaryIO | TextIO | None = None,
    cache_logger_on_first_use: bool = True,
    sending_to_supervisor: bool = False,
    colored_console_log: bool | None = None,
):
    """Set up struct logging and stdlib logging config."""
    from airflow.configuration import conf

    if log_level == "DEFAULT":
        log_level = "INFO"

        log_level = conf.get("logging", "logging_level", fallback="INFO")

    # If colored_console_log is not explicitly set, read from configuration
    if colored_console_log is None:
        colored_console_log = conf.getboolean("logging", "colored_console_log", fallback=True)

    from airflow.sdk._shared.logging import configure_logging, translate_config_values

    log_fmt, callsite_params = translate_config_values(
        log_format=conf.get("logging", "log_format"),
        callsite_params=conf.getlist("logging", "callsite_parameters", fallback=[]),
    )

    mask_secrets = not sending_to_supervisor
    extra_processors: tuple[Processor, ...] = ()

    if mask_secrets:
        extra_processors += (mask_logs,)

    if (remote := load_remote_log_handler()) and (remote_processors := getattr(remote, "processors")):
        extra_processors += remote_processors

    configure_logging(
        json_output=json_output,
        log_level=log_level,
        log_format=log_fmt,
        output=output,
        cache_logger_on_first_use=cache_logger_on_first_use,
        colors=colored_console_log,
        extra_processors=extra_processors,
        callsite_parameters=callsite_params,
    )

    global _warnings_showwarning

    if _warnings_showwarning is None:
        _warnings_showwarning = warnings.showwarning
        # Capture warnings and show them via structlog -- i.e. in task logs
        warnings.showwarning = _showwarning


def logger_at_level(name: str, level: int) -> Logger:
    """Create a new logger at the given level."""
    from airflow.sdk._shared.logging.structlog import LEVEL_TO_FILTERING_LOGGER

    return structlog.wrap_logger(
        None, wrapper_class=LEVEL_TO_FILTERING_LOGGER[level], logger_factory_args=(name)
    )


def init_log_file(local_relative_path: str) -> Path:
    """
    Ensure log file and parent directories are created.

    Any directories that are missing are created with the right permission bits.
    """
    from airflow.configuration import conf
    from airflow.sdk._shared.logging import init_log_file

    new_file_permissions = int(
        conf.get("logging", "file_task_handler_new_file_permissions", fallback="0o664"),
        8,
    )
    new_folder_permissions = int(
        conf.get("logging", "file_task_handler_new_folder_permissions", fallback="0o775"),
        8,
    )

    base_log_folder = conf.get("logging", "base_log_folder")

    return init_log_file(
        base_log_folder,
        local_relative_path,
        new_folder_permissions=new_folder_permissions,
        new_file_permissions=new_file_permissions,
    )


def load_remote_log_handler() -> RemoteLogIO | None:
    import airflow.logging_config

    return airflow.logging_config.REMOTE_TASK_LOG


def load_remote_conn_id() -> str | None:
    import airflow.logging_config
    from airflow.configuration import conf

    if conn_id := conf.get("logging", "remote_log_conn_id", fallback=None):
        return conn_id

    return airflow.logging_config.DEFAULT_REMOTE_CONN_ID


def relative_path_from_logger(logger) -> Path | None:
    if not logger:
        return None
    if not hasattr(logger, "_file"):
        logger.warning("Unable to find log file, logger was of unexpected type", type=type(logger))
        return None

    fh = logger._file
    fname = fh.name

    if fh.fileno() == 1 or not isinstance(fname, str):
        # Logging to stdout, or something odd about this logger, don't try to upload!
        return None
    from airflow.configuration import conf

    base_log_folder = conf.get("logging", "base_log_folder")
    return Path(fname).relative_to(base_log_folder)


def upload_to_remote(logger: FilteringBoundLogger, ti: RuntimeTI):
    raw_logger = getattr(logger, "_logger")

    handler = load_remote_log_handler()
    if not handler:
        return

    try:
        relative_path = relative_path_from_logger(raw_logger)
    except Exception:
        return
    if not relative_path:
        return

    log_relative_path = relative_path.as_posix()
    handler.upload(log_relative_path, ti)


def mask_secret(secret: JsonValue, name: str | None = None) -> None:
    """
    Mask a secret in both task process and supervisor process.

    For secrets loaded from backends (Vault, env vars, etc.), this ensures
    they're masked in both the task subprocess AND supervisor's log output.
    Works safely in both sync and async contexts.
    """
    from contextlib import suppress

    from airflow.sdk._shared.secrets_masker import _secrets_masker

    _secrets_masker().add_mask(secret, name)

    with suppress(Exception):
        # Try to tell supervisor (only if in task execution context)
        from airflow.sdk.execution_time import task_runner
        from airflow.sdk.execution_time.comms import MaskSecret

        if comms := getattr(task_runner, "SUPERVISOR_COMMS", None):
            comms.send(MaskSecret(value=secret, name=name))


def reset_logging():
    """
    Convince for testing. Not for production use.

    :meta private:
    """
    from airflow.sdk._shared.logging.structlog import structlog_processors

    global _warnings_showwarning
    if _warnings_showwarning is not None:
        warnings.showwarning = _warnings_showwarning
        _warnings_showwarning = None

    structlog_processors.cache_clear()
    logging_processors.cache_clear()


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
        from airflow.sdk._shared.logging.structlog import logger_without_processor_of_type

        log = logger_without_processor_of_type(
            structlog.get_logger("py.warnings").bind(), structlog.processors.CallsiteParameterAdder
        )

        log.warning(str(message), category=category.__name__, filename=filename, lineno=lineno)
