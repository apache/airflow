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
"""Compatibility shims so the provider runs on both Airflow 2 and Airflow 3.

Kept as a self-contained module per Airflow's provider conventions; it must not
be imported by other providers.
"""

from __future__ import annotations

from airflow import __version__ as _airflow_version
from packaging.version import Version

AIRFLOW_V_3_0_PLUS = Version(Version(_airflow_version).base_version) >= Version("3.0.0")

# BaseHook moved to the Task SDK in Airflow 3; fall back to the 2.x location.
try:
    from airflow.sdk.bases.hook import BaseHook
except ModuleNotFoundError:
    from airflow.hooks.base import BaseHook  # type: ignore[no-redef]

__all__ = ["AIRFLOW_V_3_0_PLUS", "BaseHook"]
