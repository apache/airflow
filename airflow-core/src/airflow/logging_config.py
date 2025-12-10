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
from importlib import import_module
from typing import TYPE_CHECKING, Any

from airflow._shared.module_loading import import_string
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException

if TYPE_CHECKING:
    from airflow.logging.remote import RemoteLogIO

log = logging.getLogger(__name__)


REMOTE_TASK_LOG: RemoteLogIO | None
DEFAULT_REMOTE_CONN_ID: str | None = None


def __getattr__(name: str):
    if name == "REMOTE_TASK_LOG":
        load_logging_config()
        return REMOTE_TASK_LOG


def load_logging_config() -> tuple[dict[str, Any], str]:
    """Configure & Validate Airflow Logging."""
    global REMOTE_TASK_LOG, DEFAULT_REMOTE_CONN_ID
    fallback = "airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG"
    logging_class_path = conf.get("logging", "logging_config_class", fallback=fallback)

    # Sometimes we end up with `""` as the value!
    logging_class_path = logging_class_path or fallback

    user_defined = logging_class_path != fallback

    try:
        logging_config = import_string(logging_class_path)

        # Make sure that the variable is in scope
        if not isinstance(logging_config, dict):
            raise ValueError("Logging Config should be of dict type")

        if user_defined:
            log.info("Successfully imported user-defined logging config from %s", logging_class_path)

    except Exception as err:
        # Import default logging configurations.
        raise ImportError(
            f"Unable to load {'custom ' if user_defined else ''}logging config from {logging_class_path} due "
            f"to: {type(err).__name__}:{err}"
        )
    else:
        modpath = logging_class_path.rsplit(".", 1)[0]
        try:
            mod = import_module(modpath)

            # Load remote logging configuration from the custom module
            REMOTE_TASK_LOG = getattr(mod, "REMOTE_TASK_LOG")
            DEFAULT_REMOTE_CONN_ID = getattr(mod, "DEFAULT_REMOTE_CONN_ID", None)
        except Exception as err:
            log.info("Remote task logs will not be available due to an error:  %s", err)

    return logging_config, logging_class_path


def configure_logging():
    from airflow._shared.logging import configure_logging, init_log_folder, translate_config_values

    logging_config, logging_class_path = load_logging_config()
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
        configure_logging(
            log_level=level,
            namespace_log_levels=conf.get("logging", "namespace_levels", fallback=None),
            stdlib_config=logging_config,
            log_format=log_fmt,
            callsite_parameters=callsite_params,
            colors=colors,
        )
    except (ValueError, KeyError) as e:
        log.error("Unable to load the config, contains a configuration error.")
        # When there is an error in the config, escalate the exception
        # otherwise Airflow would silently fall back on the default config
        raise e

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
