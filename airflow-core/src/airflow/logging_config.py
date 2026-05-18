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
import warnings
from typing import TYPE_CHECKING, Any

from airflow._shared.logging.factory import DEFAULT_LOGGING_CONFIG_PATH, resolve_remote_task_log
from airflow._shared.module_loading import import_string
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException

if TYPE_CHECKING:
    from airflow.logging.remote import RemoteLogIO

log = logging.getLogger(__name__)


class _ActiveLoggingConfig:
    """Private class to hold active logging config variables."""

    logging_config_loaded: bool = False
    remote_task_log: RemoteLogIO | None
    default_remote_conn_id: str | None = None

    @classmethod
    def set(cls, remote_task_log: RemoteLogIO | None, default_remote_conn_id: str | None) -> None:
        """Set remote logging configuration atomically."""
        cls.remote_task_log = remote_task_log
        cls.default_remote_conn_id = default_remote_conn_id
        cls.logging_config_loaded = True


def get_remote_task_log() -> RemoteLogIO | None:
    if not _ActiveLoggingConfig.logging_config_loaded:
        _load_logging_config()
    return _ActiveLoggingConfig.remote_task_log


def get_default_remote_conn_id() -> str | None:
    if conn_id := conf.get("logging", "remote_log_conn_id", fallback=None):
        return conn_id

    if not _ActiveLoggingConfig.logging_config_loaded:
        _load_logging_config()
    return _ActiveLoggingConfig.default_remote_conn_id


def _get_logging_config() -> dict[str, Any]:
    """Import and validate the ``[logging] logging_config_class`` dict."""
    logging_class_path = (
        conf.get("logging", "logging_config_class", fallback=DEFAULT_LOGGING_CONFIG_PATH)
        or DEFAULT_LOGGING_CONFIG_PATH
    )
    user_defined = logging_class_path != DEFAULT_LOGGING_CONFIG_PATH

    try:
        logging_config = import_string(logging_class_path)

        # Make sure that the variable is in scope
        if not isinstance(logging_config, dict):
            raise ValueError("Logging Config should be of dict type")

        if user_defined:
            log.info("Successfully imported user-defined logging config from %s", logging_class_path)
    except Exception as err:
        raise ImportError(
            f"Unable to load {'custom ' if user_defined else ''}logging config from {logging_class_path} due "
            f"to: {type(err).__name__}:{err}"
        )

    return logging_config


def _load_logging_config() -> None:
    """Load and cache the remote logging configuration from core config."""
    from airflow.providers_manager import ProvidersManager

    remote_task_log, default_remote_conn_id = resolve_remote_task_log(
        conf=conf,
        providers_manager=ProvidersManager(),
        import_string=import_string,
    )
    _ActiveLoggingConfig.set(remote_task_log, default_remote_conn_id)


def load_logging_config() -> tuple[dict[str, Any], str]:
    """
    Import the logging config dict and load the remote logging handler.

    .. deprecated::
        Use :func:`_get_logging_config` for the logging dict and
        :func:`_load_logging_config` for remote handler setup.
    """
    warnings.warn(
        "load_logging_config is deprecated; use _get_logging_config() for the logging dict "
        "and _load_logging_config() for remote handler setup.",
        DeprecationWarning,
        stacklevel=2,
    )
    _load_logging_config()
    return _get_logging_config(), conf.get(
        "logging", "logging_config_class", fallback=DEFAULT_LOGGING_CONFIG_PATH
    ) or DEFAULT_LOGGING_CONFIG_PATH


def _warn_if_missing_remote_task_log() -> None:
    """
    Warn if ``[logging] remote_logging`` is on but the user module exposes no remote IO.

    Runs *after* ``dictConfig`` has constructed handlers, so deprecated
    self-registration in provider task handlers (Elasticsearch, OpenSearch) has
    already had its chance to populate ``_ActiveLoggingConfig.remote_task_log``.
    Only fires for user-defined ``logging_config_class`` values; the stock
    fallback is exempt.
    """
    logging_class_path = (
        conf.get("logging", "logging_config_class", fallback=DEFAULT_LOGGING_CONFIG_PATH)
        or DEFAULT_LOGGING_CONFIG_PATH
    )
    user_defined = bool(logging_class_path) and logging_class_path != DEFAULT_LOGGING_CONFIG_PATH
    remote_logging_enabled = conf.getboolean("logging", "remote_logging", fallback=False)
    if not (user_defined and remote_logging_enabled):
        return
    if _ActiveLoggingConfig.remote_task_log is not None:
        return
    # Strip the trailing ``.<config_attr>`` to leave the enclosing module path.
    # ``logging_class_path`` should always be dotted since ``import_string``
    # would have raised otherwise, but guard the access defensively.
    parts = logging_class_path.rsplit(".", 1)
    modpath = parts[0] if len(parts) == 2 else logging_class_path
    log.warning(
        "[logging] remote_logging is enabled but the user-defined logging module %r "
        "does not expose a REMOTE_TASK_LOG attribute, so remote task-log read-back is "
        "disabled. Define REMOTE_TASK_LOG (a RemoteLogIO instance) at module scope "
        "to enable it.",
        modpath,
    )


def configure_logging():
    from airflow._shared.logging import configure_logging, init_log_folder, translate_config_values

    logging_config = _get_logging_config()
    try:
        level: str = getattr(
            logging_config, "LOG_LEVEL", conf.get("logging", "logging_level", fallback="INFO")
        ).upper()

        colors = getattr(
            logging_config,
            "COLORED_LOG",
            conf.getboolean("logging", "colored_console_log", fallback=True),
        )
        # Try to init logging

        log_fmt, callsite_params = translate_config_values(
            log_format=getattr(logging_config, "LOG_FORMAT", conf.get("logging", "log_format", fallback="")),
            callsite_params=conf.getlist("logging", "callsite_parameters", fallback=[]),
        )
        json_output = conf.getboolean("logging", "json_logs", fallback=False)

        stdlib_config = dict(logging_config)
        # Route uvicorn/gunicorn error loggers explicitly through our handler so their output
        # is formatted correctly regardless of what propagation state those loggers end up in.
        # Suppress the built-in access loggers; HttpAccessLogMiddleware and
        # AirflowUvicornWorker.CONFIG_KWARGS take over access logging instead.
        extra_loggers = {
            "uvicorn.access": {"handlers": [], "propagate": False},
            "gunicorn.access": {"handlers": [], "propagate": False},
            "uvicorn.error": {"handlers": ["default"], "propagate": False},
            "gunicorn.error": {"handlers": ["default"], "propagate": False},
        }
        stdlib_config = {**stdlib_config, "loggers": {**stdlib_config.get("loggers", {}), **extra_loggers}}

        configure_logging(
            log_level=level,
            namespace_log_levels=conf.get("logging", "namespace_levels", fallback=None),
            stdlib_config=stdlib_config,
            log_format=log_fmt,
            log_timestamp_format=conf.get("logging", "log_timestamp_format", fallback="iso"),
            callsite_parameters=callsite_params,
            colors=colors,
            json_output=json_output,
        )
    except (ValueError, KeyError) as e:
        log.error("Unable to load the config, contains a configuration error.")
        # When there is an error in the config, escalate the exception
        # otherwise Airflow would silently fall back on the default config
        raise e

    # Runs after dictConfig so deprecated handler self-registration (ES/OS) has
    # had its chance to populate _ActiveLoggingConfig.remote_task_log.
    _warn_if_missing_remote_task_log()

    validate_logging_config()

    new_folder_permissions = int(
        conf.get("logging", "file_task_handler_new_folder_permissions", fallback="0o775"),
        8,
    )

    base_log_folder = conf.get("logging", "base_log_folder")

    return init_log_folder(
        base_log_folder,
        new_folder_permissions=new_folder_permissions,
    )


def validate_logging_config():
    """Validate the provided Logging Config."""
    # Now lets validate the other logging-related settings
    task_log_reader = conf.get("logging", "task_log_reader")

    logger = logging.getLogger("airflow.task")

    def _get_handler(name):
        return next((h for h in logger.handlers if h.name == name), None)

    if _get_handler(task_log_reader) is None:
        # Check for pre 1.10 setting that might be in deployed airflow.cfg files
        if task_log_reader == "file.task" and _get_handler("task"):
            warnings.warn(
                f"task_log_reader setting in [logging] has a deprecated value of {task_log_reader!r}, "
                "but no handler with this name was found. Please update your config to use task. "
                "Running config has been adjusted to match",
                DeprecationWarning,
                stacklevel=2,
            )
            conf.set("logging", "task_log_reader", "task")
        else:
            raise AirflowConfigException(
                f"Configured task_log_reader {task_log_reader!r} was not a handler of "
                f"the 'airflow.task' logger."
            )
