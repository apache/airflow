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

from datetime import timedelta
from typing import TYPE_CHECKING, Callable, Mapping

from airflow.configuration import conf
from airflow.dag_processing.dag_ingester import DagIngester
from airflow.dag_processing.manager import DagFileProcessorManager

if TYPE_CHECKING:
    from airflow.dag_processing.dag_store import DagStore
    from airflow.dag_processing.dag_importer import DagImporter


class ContinuousIngester(DagIngester):
    def run_ingestion(self, dag_store: DagStore, importers: Mapping[str, DagImporter], heartbeat_callback: Callable[[], None] | None = None) -> None:
        self.log.info("Starting Continuous Ingester: %s", importers)
        processor_timeout_seconds: int = conf.getint("core", "dag_file_processor_timeout")
        processor_timeout = timedelta(seconds=processor_timeout_seconds)
        processor_manager = DagFileProcessorManager(processor_timeout=processor_timeout, 
            importers = importers,
            dag_store=dag_store,
            max_runs=-1,
            dag_ids=[],
            pickle_dags=False,
        )
        processor_manager.heartbeat = heartbeat_callback or (lambda: None)
        processor_manager.start()

    def supports_priority_parsing(self, subpath: str):
        return True