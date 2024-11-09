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
"""Compatibility layer for API to provide both FastAPI as wenn as Connexion based endpoints."""

from __future__ import annotations

from packaging.version import Version

from airflow import __version__ as airflow_version

AIRFLOW_VERSION = Version(airflow_version)
AIRFLOW_V_3_0_PLUS = Version(AIRFLOW_VERSION.base_version) >= Version("3.0.0")

if AIRFLOW_V_3_0_PLUS:
    from airflow.api_fastapi.common.router import AirflowRouter

    AirflowRouter = AirflowRouter
else:
    from typing import Callable

    class AirflowRouter:  # type: ignore[no-redef]
        def __init__(self, *_, **__):
            pass

        def get(self, *_):
            def decorator(func: Callable) -> Callable:
                return func

            return decorator
