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

from airflow import settings
from airflow.dag_processing.bundles.base import BaseDagBundle


class LocalDagBundle(BaseDagBundle):
    """
    Local DAG bundle - exposes a local directory as a DAG bundle.

    :param path: Local path where the DAGs are stored
    """

    supports_versioning = False

    def __init__(self, *, path: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        if path is None:
            path = settings.DAGS_FOLDER

        self._path = Path(path)

    def get_current_version(self) -> None:
        return None

    def refresh(self) -> None:
        """Nothing to refresh - it's just a local directory."""

    @property
    def path(self) -> Path:
        return self._path
