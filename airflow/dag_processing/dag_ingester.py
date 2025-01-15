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

from abc import abstractmethod
from typing import TYPE_CHECKING, Callable, Iterable, Mapping

from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.dag_processing.dag_store import DagStore
    from airflow.dag_processing.dag_importer import DagImporter


class DagIngester(LoggingMixin):

    @abstractmethod
    def run_ingestion(self, 
        dag_store: DagStore, 
        importers: Mapping[str, DagImporter], 
        heartbeat_callback: Callable[[], None] | None = None
    ) -> None:
        """
        Run ingestion policy.
        """
        ...

    @abstractmethod
    def supports_priority_parsing(self, subpath: str) -> bool:
        """
        Whether DAG ingester supports priority parsing for a given subpath.

        If not, re-parsing request in Airflow UI is disabled for DAGs using this ingester.
        """
        ...
