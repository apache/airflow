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
"""Version compatibility utilities for Great Expectations provider."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from packaging.version import Version

__all__ = ["AIRFLOW_V_2_10_PLUS"]

try:
    from airflow import __version__ as airflow_version
except ImportError:
    airflow_version = "0.0.0"

try:
    from packaging.version import Version
    
    _AIRFLOW_VERSION: Version = Version(airflow_version)
    AIRFLOW_V_2_10_PLUS = _AIRFLOW_VERSION >= Version("2.10.0")
except Exception:
    # If we can't parse the version, assume it's a dev version
    AIRFLOW_V_2_10_PLUS = True
    warnings.warn(
        f"Unable to parse Airflow version {airflow_version}. "
        "Assuming version 2.10.0 or higher.",
        UserWarning,
        stacklevel=2,
    ) 