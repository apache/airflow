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

from collections.abc import Callable
from enum import Enum
from functools import wraps
from typing import ParamSpec, TypeVar

import structlog

from airflow.configuration import AIRFLOW_HOME, conf

# Type variables for preserving function signatures
PS = ParamSpec("PS")
RT = TypeVar("RT")

log = structlog.get_logger(logger_name=__name__)


class MemrayTraceComponents(Enum):
    """Possible airflow components can apply memray trace."""

    scheduler = "scheduler"
    dag_processor = "dag_processor"
    api = "api"


def enable_memray_trace(component: MemrayTraceComponents) -> Callable[[Callable[PS, RT]], Callable[PS, RT]]:
    """
    Conditionally track memory using memray based on configuration.

    Args:
        component: Enum value of the component for configuration lookup
    """

    def decorator(func: Callable[PS, RT]) -> Callable[PS, RT]:
        @wraps(func)
        def wrapper(*args: PS.args, **kwargs: PS.kwargs) -> RT:  # type: ignore[return]
            _memray_trace_components = conf.getenumlist(
                "profiling", "memray_trace_components", MemrayTraceComponents
            )
            if component not in _memray_trace_components:
                return func(*args, **kwargs)

            try:
                import memray

                profile_path = f"{AIRFLOW_HOME}/{component.value}_memory.bin"
                with memray.Tracker(
                    profile_path,
                ):
                    log.info("Memray tracing enabled for %s. Output: %s", component.value, profile_path)
                    return func(*args, **kwargs)
            except ImportError as error:
                # Silently fall back to running without tracking
                log.warning(
                    "ImportError memray.Tracker: %s in %s, please check the memray is installed",
                    error.msg,
                    component.value,
                )
                return func(*args, **kwargs)
            except Exception as exception:
                log.warning("Fail to apply memray.Tracker in %s, error: %s", component.value, exception)
                return func(*args, **kwargs)

        return wrapper

    return decorator
