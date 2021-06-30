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
#
"""
An Action Logger module. Singleton pattern has been applied into this module
so that registered callbacks can be used all through the same python process.
"""

import logging
from typing import Callable, List

from airflow.utils.session import create_session


def register_pre_exec_callback(action_logger):
    """
    Registers more action_logger function callback for pre-execution.
    This function callback is expected to be called with keyword args.
    For more about the arguments that is being passed to the callback,
    refer to airflow.utils.cli.action_logging()

    :param action_logger: An action logger function
    :return: None
    """
    logging.debug("Adding %s to pre execution callback", action_logger)
    __pre_exec_callbacks.append(action_logger)


def register_post_exec_callback(action_logger):
    """
    Registers more action_logger function callback for post-execution.
    This function callback is expected to be called with keyword args.
    For more about the arguments that is being passed to the callback,
    refer to airflow.utils.cli.action_logging()

    :param action_logger: An action logger function
    :return: None
    """
    logging.debug("Adding %s to post execution callback", action_logger)
    __post_exec_callbacks.append(action_logger)


def on_pre_execution(**kwargs):
    """
    Calls callbacks before execution.
    Note that any exception from callback will be logged but won't be propagated.

    :param kwargs:
    :return: None
    """
    logging.debug("Calling callbacks: %s", __pre_exec_callbacks)
    for callback in __pre_exec_callbacks:
        try:
            callback(**kwargs)
        except Exception:
            logging.exception('Failed on pre-execution callback using %s', callback)


def on_post_execution(**kwargs):
    """
    Calls callbacks after execution.
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
            logging.exception('Failed on post-execution callback using %s', callback)


def default_action_log(log, **_):
    """
    A default action logger callback that behave same as www.utils.action_logging
    which uses global session and pushes log ORM object.

    :param log: An log ORM instance
    :param **_: other keyword arguments that is not being used by this function
    :return: None
    """
    try:
        with create_session() as session:
            session.add(log)
    except Exception as error:
        logging.warning("Failed to log action with %s", error)


__pre_exec_callbacks = []  # type: List[Callable]
__post_exec_callbacks = []  # type: List[Callable]

# By default, register default action log into pre-execution callback
register_pre_exec_callback(default_action_log)
