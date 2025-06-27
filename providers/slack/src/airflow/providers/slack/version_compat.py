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

from functools import cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.version import version

    # noinspection PyUnusedLocal
    version = version


@cache
def get_base_airflow_version_tuple() -> tuple[int, int, int]:
    """
    Get the base Airflow version tuple from the airflow.version module.

    :return: Tuple of (major, minor, micro) version numbers
    """
    try:
        from airflow.version import version

        parts = version.split(".")[:3]
        # Ensure we always have exactly 3 parts, padding with "0" if necessary
        while len(parts) < 3:
            parts.append("0")
        return (int(parts[0]), int(parts[1]), int(parts[2]))
    except (ImportError, ValueError):
        # If we can't import the version or parse it, assume we're running
        # on Airflow 2.x to maintain backward compatibility
        return (2, 0, 0)


AIRFLOW_V_3_0_PLUS = get_base_airflow_version_tuple() >= (3, 0, 0)
