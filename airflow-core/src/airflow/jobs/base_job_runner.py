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

from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.jobs.job import Job


class BaseJobRunner:
    """Abstract class for job runners to derive from."""

    job_type = "undefined"

    def __init__(self, job: Job) -> None:
        if job.job_type and job.job_type != self.job_type:
            raise AirflowException(
                f"The job is already assigned a different job_type: {job.job_type}."
                f"This is a bug and should be reported."
            )
        job.job_type = self.job_type
        self.job: Job = job

    def _execute(self) -> int | None:
        """
        Execute the logic connected to the runner.

        This method should be overridden by subclasses.

        :meta private:
        :return: return code if available, otherwise None
        """
        raise NotImplementedError()

    @provide_session
    def heartbeat_callback(self, session: Session = NEW_SESSION) -> None:
        """
        Execute callback during heartbeat.

        This method can be overwritten by the runners.
        """

    @classmethod
    @provide_session
    def most_recent_job(cls, session: Session = NEW_SESSION) -> Job | None:
        """Return the most recent job of this type, if any, based on last heartbeat received."""
        from airflow.jobs.job import most_recent_job

        return most_recent_job(cls.job_type, session=session)
