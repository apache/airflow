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

import itertools
import pathlib
from typing import TYPE_CHECKING, Callable, Generator

from airflow.configuration import conf
from airflow.dag_processing.dag_store import DagStore
from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.dag_processing.dag_importer import DagImporter, DagsImportResult, ImportOptions
    from airflow.dag_processing.dag_ingester import DagIngester


class DagParser:
    def __init__(self):
        # path -> importer
        self.importers: dict[str, DagImporter] = {}
        self.ingester: DagIngester = None
        self._load_parsers_configuration()

    def run_ingestion(self, heartbeat_callback: Callable[[], None] | None = None) -> None:
        """
        Run ingestion as configured in the ingester.

        Used by dag-processor.
        """
        dag_store = DagStore()
        self.ingester.run_ingestion(dag_store, self.importers, heartbeat_callback)

    def parse_paths(self, subdir: str | None = None, options: ImportOptions | None = None) -> Generator[DagsImportResult, None, None]:
        """
        Parse paths with importer covering this path. Used by airflow workers to get an executable task instance of a DAG.
        """
        if not subdir:
            return itertools.chain(*[importer.import_path(path, options) for path, importer in self.importers.items()])
        subpath = pathlib.Path(subdir)
        for covered_path in self.importers:
            # TODO: This assumes that all paths are non-overlapping by design.
            if subpath.is_relative_to(covered_path):
                return self.importers[covered_path].import_path(subdir, options)

        raise AirflowException(f"Unknown subdir: {subdir}")

    def _load_parsers_configuration(self) -> None:
        from airflow import settings
        
        from airflow.dag_processing.fs_dag_importer import FSDagImporter
        from airflow.providers.google.common.importers.notebooks_importer import NotebooksImporter
        from airflow.dag_processing.example_dag_importer import ExampleDagImporter

        # TODO: depend on configuration, so "deployment manager" user can customize it,
        # here it is static for demonstration purpose only. (Can be similar to "bundle" configuration).
        self.importers = {
            settings.DAGS_FOLDER: FSDagImporter(),
            '/files/notebooks': NotebooksImporter(),
        }
        if conf.getboolean("core", "LOAD_EXAMPLES"):
            from airflow.dag_processing.example_dag_importer import example_dags_path
            self.importers[example_dags_path()] = ExampleDagImporter()
        ingester_mode = conf.get("scheduler", "ingester_mode")
        if ingester_mode == "continuous":
            from airflow.dag_processing.continuous_ingester import ContinuousIngester
            self.ingester = ContinuousIngester()
        elif ingester_mode == "once":
            from airflow.dag_processing.once_ingester import OnceIngester
            self.ingester = OnceIngester()
        else:
            raise ValueError(f"Unknown ingester mode: {ingester_mode}")
