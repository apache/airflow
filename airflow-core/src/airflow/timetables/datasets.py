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

import warnings

from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.utils.deprecation_tools import DeprecatedImportWarning


class DatasetOrTimeSchedule:
    """Deprecated alias for `AssetOrTimeSchedule`."""

    def __new__(cls, *, timetable, datasets) -> AssetOrTimeSchedule:  # type: ignore[misc]
        warnings.warn(
            "DatasetOrTimeSchedule is deprecated and will be removed in Airflow 3.2. Use `airflow.timetables.AssetOrTimeSchedule` instead.",
            DeprecatedImportWarning,
            stacklevel=2,
        )
        return AssetOrTimeSchedule(timetable=timetable, assets=datasets)
