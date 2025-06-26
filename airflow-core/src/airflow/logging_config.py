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
from logging.config import dictConfig
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from airflow.logging.remote import RemoteLogIO

log = logging.getLogger(__name__)


REMOTE_TASK_LOG: RemoteLogIO | None


def __getattr__(name: str):
    if name == "REMOTE_TASK_LOG":
        global REMOTE_TASK_LOG
        load_logging_config()
        return REMOTE_TASK_LOG


def load_logging_config() -> tuple[dict[str, Any], str]:
    """Configure & Validate Airflow Logging."""
    global REMOTE_TASK_LOG
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
        mod = logging_class_path.rsplit(".", 1)[0]
        try:
            remote_task_log = import_string(f"{mod}.REMOTE_TASK_LOG")
            REMOTE_TASK_LOG = remote_task_log
        except Exception as err:
            log.info("Remote task logs will not be available due to an error:  %s", err)

    return logging_config, logging_class_path


def configure_logging():
    logging_config, logging_class_path = load_logging_config()
    try:
        # Ensure that the password masking filter is applied to the 'task' handler
        # no matter what the user did.
        if "filters" in logging_config and "mask_secrets" in logging_config["filters"]:
            # But if they replace the logging config _entirely_, don't try to set this, it won't work
            task_handler_config = logging_config["handlers"]["task"]

            task_handler_config.setdefault("filters", [])

            if "mask_secrets" not in task_handler_config["filters"]:
                task_handler_config["filters"].append("mask_secrets")

        # Try to init logging
        dictConfig(logging_config)
    except (ValueError, KeyError) as e:
        log.error("Unable to load the config, contains a configuration error.")
        # When there is an error in the config, escalate the exception
        # otherwise Airflow would silently fall back on the default config
        raise e

    validate_logging_config(logging_config)

    return logging_class_path


def validate_logging_config(logging_config):
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
