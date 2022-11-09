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

import functools
import logging
from inspect import signature
from typing import Any

from sqlalchemy.exc import DBAPIError, OperationalError

from airflow.configuration import conf

MAX_DB_RETRIES = conf.getint('database', 'max_db_retries', fallback=3)


def run_with_db_retries(max_retries: int = MAX_DB_RETRIES, logger: logging.Logger | None = None, **kwargs):
    """Return Tenacity Retrying object with project specific default"""
    import tenacity

    # Default kwargs
    retry_kwargs = dict(
        retry=tenacity.retry_if_exception_type(exception_types=(OperationalError, DBAPIError)),
        wait=tenacity.wait_random_exponential(multiplier=0.5, max=5),
        stop=tenacity.stop_after_attempt(max_retries),
        reraise=True,
        **kwargs,
    )
    if logger and isinstance(logger, logging.Logger):
        retry_kwargs["before_sleep"] = tenacity.before_sleep_log(logger, logging.DEBUG, True)

    return tenacity.Retrying(**retry_kwargs)


def retry_db_transaction(_func: Any = None, retries: int = MAX_DB_RETRIES, **retry_kwargs):
    """
    Decorator to retry Class Methods and Functions in case of ``OperationalError`` from DB.
    It should not be used with ``@provide_session``.
    """

    def retry_decorator(func):
        # Get Positional argument for 'session'
        func_params = signature(func).parameters
        try:
            # func_params is an ordered dict -- this is the "recommended" way of getting the position
            session_args_idx = tuple(func_params).index("session")
        except ValueError:
            raise ValueError(f"Function {func.__qualname__} has no `session` argument")
        # We don't need this anymore -- ensure we don't keep a reference to it by mistake
        del func_params

        @functools.wraps(func)
        def wrapped_function(*args, **kwargs):
            logger = args[0].log if args and hasattr(args[0], "log") else logging.getLogger(func.__module__)

            # Get session from args or kwargs
            if "session" in kwargs:
                session = kwargs["session"]
            elif len(args) > session_args_idx:
                session = args[session_args_idx]
            else:
                raise TypeError(f"session is a required argument for {func.__qualname__}")

            for attempt in run_with_db_retries(max_retries=retries, logger=logger, **retry_kwargs):
                with attempt:
                    logger.debug(
                        "Running %s with retries. Try %d of %d",
                        func.__qualname__,
                        attempt.retry_state.attempt_number,
                        retries,
                    )
                    try:
                        return func(*args, **kwargs)
                    except OperationalError:
                        session.rollback()
                        raise

        return wrapped_function

    # Allow using decorator with and without arguments
    if _func is None:
        return retry_decorator
    else:
        return retry_decorator(_func)
