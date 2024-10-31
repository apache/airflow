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

from typing import TYPE_CHECKING

from airflow import __version__ as AIRFLOW_VERSION

if TYPE_CHECKING:
    from airflow.providers.standard.operators.python import (
        _SERIALIZERS,
        PythonOperator,
        ShortCircuitOperator,
        get_current_context,
    )
else:
    try:
        from airflow.providers.standard.operators.python import (
            _SERIALIZERS,
            PythonOperator,
            ShortCircuitOperator,
            get_current_context,
        )
    except ModuleNotFoundError:
        from packaging.version import Version

        _IS_AIRFLOW_2_10_OR_HIGHER = Version(Version(AIRFLOW_VERSION).base_version) >= Version("2.10.0")

        from airflow.operators.python import (
            PythonOperator,
            ShortCircuitOperator,
            get_current_context,
        )

        if _IS_AIRFLOW_2_10_OR_HIGHER:
            from airflow.operators.python import _SERIALIZERS


__all__ = ["PythonOperator", "_SERIALIZERS", "ShortCircuitOperator", "get_current_context"]
