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

import json
from dataclasses import asdict, dataclass
from multiprocessing import Process
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from psutil import Popen

    from airflow.providers.edge.models.edge_worker import EdgeWorkerState
    from airflow.providers.edge.worker_api.datamodels import EdgeJobFetched


@dataclass
class MaintenanceMarker:
    """Maintenance mode status."""

    maintenance: str
    comments: str | None

    @property
    def json(self) -> str:
        """Get the maintenance status as JSON."""
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(json_str: str) -> MaintenanceMarker:
        """Create a Maintenance object from JSON."""
        return MaintenanceMarker(**json.loads(json_str))


@dataclass
class WorkerStatus:
    """Status of the worker."""

    job_count: int
    jobs: list
    state: EdgeWorkerState
    maintenance: bool
    maintenance_comments: str | None
    drain: bool

    @property
    def json(self) -> str:
        """Get the status as JSON."""
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(json_str: str) -> WorkerStatus:
        """Create a WorkerStatus object from JSON."""
        return WorkerStatus(**json.loads(json_str))


@dataclass
class Job:
    """Holds all information for a task/job to be executed as bundle."""

    edge_job: EdgeJobFetched
    process: Popen | Process
    logfile: Path
    logsize: int
    """Last size of log file, point of last chunk push."""

    @property
    def is_running(self) -> bool:
        """Check if the job is still running."""
        if hasattr(self.process, "returncode") and hasattr(self.process, "poll"):
            self.process.poll()
            return self.process.returncode is None
        return self.process.exitcode is None

    @property
    def is_success(self) -> bool:
        """Check if the job was successful."""
        if hasattr(self.process, "returncode"):
            return self.process.returncode == 0
        return self.process.exitcode == 0
