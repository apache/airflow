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
Module for suppress errors in Amazon Provider.

.. warning::
    Only for internal usage, this module might be changed or removed in the future
    without any further notice.

:meta: private
"""

from __future__ import annotations

import logging
from functools import wraps
from typing import Callable, TypeVar

from airflow.typing_compat import ParamSpec

PS = ParamSpec("PS")
RT = TypeVar("RT")

log = logging.getLogger(__name__)


def return_on_error(return_value: RT):
    """
    Suppress any ``Exception`` raised in decorator function.

    Main use-case when functional is optional, however any error on functions/methods might
    raise any error which are subclass of ``Exception``.

    .. note::
        Decorator doesn't intend to catch ``BaseException``,
        e.g. ``GeneratorExit``, ``KeyboardInterrupt``, ``SystemExit`` and others.

    .. warning::
        Only for internal usage, this decorator might be changed or removed in the future
        without any further notice.

    :param return_value: Return value if decorated function/method raise any ``Exception``.
    :meta: private
    """

    def decorator(func: Callable[PS, RT]) -> Callable[PS, RT]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> RT:
            try:
                return func(*args, **kwargs)
            except Exception:
                log.debug(
                    "Encountered error during execution function/method %r",
                    func.__name__,
                    exc_info=True,
                )
                return return_value

        return wrapper

    return decorator
