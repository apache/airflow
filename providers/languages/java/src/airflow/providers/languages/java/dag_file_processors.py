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
"""Java DAG file processor and locale coordinator."""

from __future__ import annotations

import contextlib
import os
from pathlib import Path

from airflow.dag_processing.processor import BaseDagFileProcessor
from airflow.providers.languages.java.coordinator import JavaLocaleCoordinator, find_main_class


class JavaDagFileProcessor(BaseDagFileProcessor):
    """
    DAG file processor for Java JAR bundle workloads.

    Registered via ``dag-file-processors`` in the Java provider's ``provider.yaml``.
    When the dag processor encounters a file that belongs to a Java bundle,
    this processor's :meth:`entrypoint` is used as the subprocess target instead
    of the default Python ``_parse_file_entrypoint``.
    """

    language = "java"

    def can_handle(self, bundle_name: str, path: str | os.PathLike[str]) -> bool:
        if not super().can_handle(bundle_name, path):
            return False

        with contextlib.suppress(FileNotFoundError):
            return find_main_class(Path(path)) is not None
        return False

    @staticmethod
    def entrypoint(path: str, bundle_name: str, bundle_path: str) -> None:
        """Bridge fd 0 (supervisor comms) to a Java subprocess over TCP."""
        JavaLocaleCoordinator.run_dag_parsing(path=path, bundle_name=bundle_name, bundle_path=bundle_path)
