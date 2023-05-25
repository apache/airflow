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

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.utils.module_loading import import_string

log = logging.getLogger(__name__)


def configure_logging():
    """Configure & Validate Airflow Logging."""
    logging_class_path = ""
    try:
        logging_class_path = conf.get("logging", "logging_config_class")
    except AirflowConfigException:
        log.debug("Could not find key logging_config_class in config")

    if logging_class_path:
        try:
            logging_config = import_string(logging_class_path)

            # Make sure that the variable is in scope
            if not isinstance(logging_config, dict):
                raise ValueError("Logging Config should be of dict type")

            log.info("Successfully imported user-defined logging config from %s", logging_class_path)
        except Exception as err:
            # Import default logging configurations.
            raise ImportError(f"Unable to load custom logging from {logging_class_path} due to {err}")
    else:
        logging_class_path = "airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG"
        logging_config = import_string(logging_class_path)
        log.debug("Unable to load custom logging, using default config instead")

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
            )
            conf.set("logging", "task_log_reader", "task")
        else:
            raise AirflowConfigException(
                f"Configured task_log_reader {task_log_reader!r} was not a handler of "
                f"the 'airflow.task' logger."
            )
