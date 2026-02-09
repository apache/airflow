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

from airflow import __version__ as airflow_version
from airflow.exceptions import AirflowProviderDeprecationWarning


def _assert_dataset_deprecation_warning(recwarn) -> None:
    if airflow_version.startswith("2"):
        warning = recwarn.pop(AirflowProviderDeprecationWarning)
        assert warning.category == AirflowProviderDeprecationWarning
        assert (
            str(warning.message)
            == "is_authorized_dataset will be renamed as is_authorized_asset in Airflow 3 and will be removed when the minimum Airflow version is set to 3.0 for the fab provider"
        )


__all__ = ["_assert_dataset_deprecation_warning"]
