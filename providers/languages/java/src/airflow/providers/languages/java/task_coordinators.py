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
"""Java task coordinator implementations."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from airflow.providers.languages.java.coordinator import JavaLocaleCoordinator

if TYPE_CHECKING:
    from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance
    from airflow.sdk.execution_time.comms import StartupDetails


class JavaTaskCoordinator:
    """Placeholder task coordinator entry point for Java workloads."""

    language = "java"

    @staticmethod
    def entrypoint(
        what: TaskInstance,
        dag_rel_path: str | os.PathLike[str],
        bundle_info: BundleInfo,
        startup_details: StartupDetails,
    ) -> None:
        """Bridge fd 0 (supervisor comms) to a Java subprocess over TCP."""
        JavaLocaleCoordinator.run_task_execution(
            what=what, dag_rel_path=dag_rel_path, bundle_info=bundle_info, startup_details=startup_details
        )
