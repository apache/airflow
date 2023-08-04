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
"""
An Action Logger module.

Singleton pattern has been applied into this module so that registered
callbacks can be used all through the same python process.
"""
from __future__ import annotations

import json
import logging
from typing import Callable


def register_pre_exec_callback(action_logger):
    """Registers more action_logger function callback for pre-execution.

    This function callback is expected to be called with keyword args.
    For more about the arguments that is being passed to the callback,
    refer to airflow.utils.cli.action_logging().

    :param action_logger: An action logger function
    :return: None
    """
    logging.debug("Adding %s to pre execution callback", action_logger)
    __pre_exec_callbacks.append(action_logger)


def register_post_exec_callback(action_logger):
    """Registers more action_logger function callback for post-execution.

    This function callback is expected to be called with keyword args.
    For more about the arguments that is being passed to the callback,
    refer to airflow.utils.cli.action_logging().

    :param action_logger: An action logger function
    :return: None
    """
    logging.debug("Adding %s to post execution callback", action_logger)
    __post_exec_callbacks.append(action_logger)


def on_pre_execution(**kwargs):
    """Calls callbacks before execution.

    Note that any exception from callback will be logged but won't be propagated.

    :param kwargs:
    :return: None
    """
    logging.debug("Calling callbacks: %s", __pre_exec_callbacks)
    for callback in __pre_exec_callbacks:
        try:
            callback(**kwargs)
        except Exception:
            logging.exception("Failed on pre-execution callback using %s", callback)


def on_post_execution(**kwargs):
    """Calls callbacks after execution.

    As it's being called after execution, it can capture status of execution,
    duration, etc. Note that any exception from callback will be logged but
    won't be propagated.

    :param kwargs:
    :return: None
    """
    logging.debug("Calling callbacks: %s", __post_exec_callbacks)
    for callback in __post_exec_callbacks:
        try:
            callback(**kwargs)
        except Exception:
            logging.exception("Failed on post-execution callback using %s", callback)


def default_action_log(sub_command, user, task_id, dag_id, execution_date, host_name, full_command, **_):
    """Default action logger callback that behaves similar to ``action_logging``.

    The difference is this function uses the global ORM session, and pushes a
    ``Log`` row into the database instead of actually logging.
    """
    from sqlalchemy.exc import OperationalError, ProgrammingError

    from airflow.models.log import Log
    from airflow.utils import timezone
    from airflow.utils.session import create_session

    try:
        with create_session() as session:
            extra = json.dumps({"host_name": host_name, "full_command": full_command})
            # Use bulk_insert_mappings here to avoid importing all models (which using the classes does) early
            # on in the CLI
            session.bulk_insert_mappings(
                Log,
                [
                    {
                        "event": f"cli_{sub_command}",
                        "task_instance": None,
                        "owner": user,
                        "extra": extra,
                        "task_id": task_id,
                        "dag_id": dag_id,
                        "execution_date": execution_date,
                        "dttm": timezone.utcnow(),
                    }
                ],
            )
    except (OperationalError, ProgrammingError) as e:
        expected = [
            '"log" does not exist',  # postgres
            "no such table",  # sqlite
            "log' doesn't exist",  # mysql
            "Invalid object name 'log'",  # mssql
        ]
        error_is_ok = e.args and any(x in e.args[0] for x in expected)
        if not error_is_ok:
            logging.warning("Failed to log action %s", e)
    except Exception as e:
        logging.warning("Failed to log action %s", e)


__pre_exec_callbacks: list[Callable] = []
__post_exec_callbacks: list[Callable] = []

# By default, register default action log into pre-execution callback
register_pre_exec_callback(default_action_log)
