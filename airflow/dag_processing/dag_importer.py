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
from dataclasses import dataclass
from typing import TYPE_CHECKING, Collection, Generator, Iterable, Mapping

from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.dag import DAG


@dataclass(frozen=True)
class DagsImportResult:
    # DAG ID -> imported DAG
    dags: Mapping[str, DAG]
    # Import path -> error message
    import_errors: Mapping[str, str]
    # Import path -> Warning
    import_warnings: Mapping[str, Collection[str]]
    # Skipped unchanged paths
    skipped_paths: Collection[str] = ()


@dataclass(frozen=True)
class ImportOptions:
    skip_unchanged: bool = False


@dataclass(frozen=True)
class PathData:
    path: str
    metadata: Mapping[str, str] | None = None


class DagImporter(LoggingMixin):
    """
    Abstract importer - piece of logic responsible of translation of abstract DAG paths into DAGs.

    In addition to being in charge of "how to create DAG from filepath", also provides metadata
    about the paths and if they contain any new changes.
    """
    @abstractmethod
    def import_path(self, dagpath: str, options: ImportOptions | None = None) -> Generator[DagsImportResult, None, None]:
        """
        Import all DAGs from a given path.

        Args:
            dagpath (str): Path to import DAGs from.
            options (ImportOptions | None): Options for DAG importing.

        Yields:
            DagsImportResult: Result of importing DAGs. Completes when all DAGs were parsed.
        """
        ...

    @abstractmethod
    def list_paths(self, subpath: str) -> Iterable[PathData]: 
        """
        List all potential DAG paths or its parents.

        Returned paths can be both individual files or directories. 
        There is no gurantee that there are any DAGs or that DAGs fileloc is exact returned path.

        The following invariants should hold true:
        - If returned 'path' contains DAGs, their fileloc should be a supath of the returned 'path'.
          Examples: path = /a/b/c.py -> DAG fileloc = /a/b/c.py
                    path = /a/x.zip -> DAG_1 fileloc = /a/x.zip/a.py; DAG_2 fileloc = /a/x.zip/b.py
        - Returned paths should be non-overlapping.
        - For every DAG exactly one path should be returned. Path can cover any number of DAGs.

        Naive universal implementation - return a single path equal to the requested subpath.

        The goal of these paths - provide the ingester paths that can be used to import DAGs in the efficient way.

        Args:
            subpath (str): Subpath to list DAG paths for.

        Returns:
            Iterable[PathData]: Iterable of PathData.
        """
        ...
