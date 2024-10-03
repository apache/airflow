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

from pathlib import Path

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.exceptions import AirflowException


class LocalDagBundle(BaseDagBundle):
    """local DAG bundle backend."""

    supports_versioning = False

    def __init__(self, *, local_folder: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._path = Path(local_folder)

    def get_current_version(self) -> str:
        raise AirflowException("Not versioned!")

    def refresh(self) -> None:
        """Nothing to refresh - it's just a local directory."""

    def cleanup(self) -> None:
        pass

    @property
    def path(self) -> Path:
        return self._path
