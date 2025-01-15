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

from typing import TYPE_CHECKING, Callable, Mapping

from airflow.dag_processing.dag_ingester import DagIngester

if TYPE_CHECKING:
    from airflow.dag_processing.dag_store import DagStore
    from airflow.dag_processing.dag_importer import DagImporter


class OnceIngester(DagIngester):
    def run_ingestion(self, 
        dag_store: DagStore,
        importers: Mapping[str, DagImporter],
        heartbeat_callback: Callable[[], None] | None = None
    ) -> None:

        for path in importers:
            # TODO: Make use of multi-processing.
            self.log.info(f'Importing DAGs from {path}...')

            path_import_errors = set()
            imported_paths = {}
            for results in importers[path].import_path(path):
                import_errors = {**results.import_errors}
                if results.dags:
                    imported_paths.update({dag.dag_id: dag.fileloc for dag in results.dags.values()})
                    store_errors = dag_store.store_dags(results.dags.values(), processor_subdir=path)
                    import_errors.update({imported_paths[dag_id]: store_errors[dag_id] for dag_id in store_errors})

                    for failed_dag_id in store_errors:
                        imported_paths.pop(failed_dag_id)

                path_import_errors.update(import_errors)
                dag_store.update_import_errors(import_errors, processor_subdir=path)
                # TODO: handle warnings
            dag_store.delete_excluded_import_errors(path_import_errors, processor_subdir=path)
            dag_store.delete_removed_entries_dags(imported_paths.values(), processor_subdir=path)
        dag_store.delete_removed_processor_subdir_dags(alive_processor_subdirs=importers)
        dag_store.delete_excluded_subdir_import_errors(set(importers.keys()))

    def supports_priority_parsing(self, subpath: str):
        return True
