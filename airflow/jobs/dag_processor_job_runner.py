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

from typing import TYPE_CHECKING, Any

from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.jobs.job import Job, perform_heartbeat
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.dag_processing.manager import DagFileProcessorManager


def empty_callback(_: Any) -> None:
    pass


class DagProcessorJobRunner(BaseJobRunner, LoggingMixin):
    """
    DagProcessorJobRunner is a job runner that runs a DagFileProcessorManager processor.

    :param job: Job instance to use
    :param processor: DagFileProcessorManager instance to use
    """

    job_type = "DagProcessorJob"

    def __init__(
        self,
        job: Job,
        processor: DagFileProcessorManager,
        *args,
        **kwargs,
    ):
        super().__init__(job)
        self.processor = processor
        self.processor.heartbeat = lambda: perform_heartbeat(
            job=self.job,
            heartbeat_callback=empty_callback,
            only_if_necessary=True,
        )

    def _execute(self) -> int | None:
        self.log.info("Starting the Dag Processor Job")
        try:
            self.processor.start()
        except Exception:
            self.log.exception("Exception when executing DagProcessorJob")
            raise
        finally:
            self.processor.terminate()
            self.processor.end()
        return None
