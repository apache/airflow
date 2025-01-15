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
import pathlib
from typing import Generator, Iterable

from airflow.dag_processing.fs_dag_importer import FSDagImporter
from airflow.dag_processing.dag_importer import DagsImportResult, DagImporter, ImportOptions, PathData
from airflow.exceptions import AirflowException


def example_dags_path():
    from airflow import example_dags

    return example_dags.__path__[0]


class ExampleDagImporter(DagImporter):
    def __init__(self):
        super().__init__()
        self.fs_importer = FSDagImporter()

    def import_path(self, dagpath: str, options: ImportOptions | None = None) -> Generator[DagsImportResult, None, None]:
        if not pathlib.Path(dagpath).is_relative_to(example_dags_path()):
            raise AirflowException(
                f"Unexpected dagpath {dagpath} for example DAG, should be a subpath of {example_dags_path()}"
            )
        return self.fs_importer.import_path(dagpath, options)

    def list_paths(self, subpath: str) -> Iterable[PathData]:
        return self.fs_importer.list_paths(subpath)
