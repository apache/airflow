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
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

from airflow.configuration import AIRFLOW_HOME, conf

# Type variables for preserving function signatures
PS = ParamSpec("PS")
RT = TypeVar("RT")

log = logging.getLogger(__name__)


def enable_memray_trace(airflow_component_name: str) -> Callable[[Callable[PS, RT]], Callable[PS, RT]]:
    """
    Conditionally track memory using memray based on configuration.

    Args:
        airflow_component_name: Name of the component for configuration lookup
    """

    def decorator(func: Callable[PS, RT]) -> Callable[PS, RT]:
        @wraps(func)
        def wrapper(*args: PS.args, **kwargs: PS.kwargs) -> RT:
            _enable_memray_trace = conf.getboolean(airflow_component_name, "enable_memray_trace")

            if not _enable_memray_trace:
                return func(*args, **kwargs)

            try:
                import memray

                profile_path = f"{AIRFLOW_HOME}/{airflow_component_name}_memory.bin"
                log.info("enable_memray_trace is on. so memory state is tracked by memray")
                with memray.Tracker(
                    profile_path,
                ):
                    log.info(
                        "Memray tracing enabled for %s. Output: %s", airflow_component_name, profile_path
                    )
                    return func(*args, **kwargs)
            except ImportError as error:
                # Silently fall back to running without tracking
                log.warning("ImportError memray.Tracker: %s", error.msg)
                return func(*args, **kwargs)

        return wrapper

    return decorator
