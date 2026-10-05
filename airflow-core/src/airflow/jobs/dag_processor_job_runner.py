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

from time import monotonic
from typing import TYPE_CHECKING

import httpx

from airflow._shared.observability.metrics import stats
from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.jobs.job import Job, JobState, execute_job, perform_heartbeat
from airflow.listeners.listener import get_listener_manager
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.dag_processing.api_client import DagProcessorAPIClient
    from airflow.dag_processing.manager import DagFileProcessorManager


class DagProcessorHeartbeatTimeout(RuntimeError):
    """The processor could not confirm its Job heartbeat within the health-check interval."""


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
        self._last_api_heartbeat = 0.0
        self._next_api_heartbeat = 0.0
        self.processor.heartbeat = lambda: perform_heartbeat(
            job=self.job,
            heartbeat_callback=self.heartbeat_callback,
            only_if_necessary=True,
        )

    def run_with_api(self, client: DagProcessorAPIClient) -> int | None:
        """Let the API own the Job row while retaining the normal processor cleanup and listeners."""
        # The client loads versioned routes which import this runner.
        from airflow.api_fastapi.execution_api.datamodels.job import TerminalJobState

        self.processor.api_client = client
        self.processor.sync_bundles(include_bundle_urls=False)
        self.job.id = client.register_job()
        self.job.state = JobState.RUNNING
        self.job.start_date = self.job.latest_heartbeat = timezone.utcnow()
        self._last_api_heartbeat = monotonic()
        self._next_api_heartbeat = self._last_api_heartbeat + self.job.heartrate
        self.processor.heartbeat = lambda: self._heartbeat_api(client)
        stats.incr("job_start", 1, 1)
        failed = True
        try:
            result = execute_job(self.job, execute_callable=self._execute)
            failed = False
            return result
        finally:
            self.job.end_date = timezone.utcnow()
            self.job.state = JobState.FAILED if failed else JobState.SUCCESS
            try:
                get_listener_manager().hook.before_stopping(component=self.job)
            except Exception:
                self.log.exception("Error calling Dag processor shutdown listener")
            try:
                client.complete_job(TerminalJobState(self.job.state))
            except Exception:
                if not failed:
                    raise
                self.log.exception("Unable to record the failed Dag processor Job")
            finally:
                stats.incr("job_end", 1, 1)

    def _heartbeat_api(self, client: DagProcessorAPIClient) -> None:
        from airflow.dag_processing.api_client import DagProcessorRegistrationRetired

        if client.restart_required:
            raise DagProcessorRegistrationRetired("The Dag processor registration requires a restart")
        now = monotonic()
        if now < self._next_api_heartbeat:
            return
        self._next_api_heartbeat = now + self.job.heartrate
        try:
            state = client.heartbeat()
        except httpx.HTTPError as error:
            if isinstance(error, httpx.HTTPStatusError) and error.response.status_code < 500:
                raise
            stats.incr("dag_processor_heartbeat_failure", 1, 1)
            if monotonic() - self._last_api_heartbeat >= conf.getint(
                "dag_processor", "health_check_threshold"
            ):
                raise DagProcessorHeartbeatTimeout("Dag processor API heartbeat timed out") from error
            self.log.warning("Unable to heartbeat the Dag processor Job; retrying on the next interval")
            return
        if client.restart_required or state != JobState.RUNNING:
            raise DagProcessorRegistrationRetired("The API requested a Dag processor restart")
        self._last_api_heartbeat = monotonic()
        self.job.latest_heartbeat = timezone.utcnow()
        self.heartbeat_callback()

    def _execute(self) -> int | None:
        self.log.info("Starting the Dag Processor Job")
        try:
            self.processor.run()
        except Exception:
            self.log.exception("Exception when executing DagProcessorJob")
            raise
        finally:
            self.processor.terminate()
            self.processor.end()
        return None

    def heartbeat_callback(self, session: Session | None = None) -> None:
        stats.incr("dag_processor_heartbeat", 1, 1)
