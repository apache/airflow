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

from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.jobs.job import Job


class BaseJobRunner:
    """Abstract class for job runners to derive from."""

    job_type = "undefined"

    def _execute(self) -> int | None:
        """
        Executes the logic connected to the runner. This method should be
        overridden by subclasses.

        :meta private:
        :return: return code if available, otherwise None
        """
        raise NotImplementedError()

    @provide_session
    def heartbeat_callback(self, session: Session = NEW_SESSION) -> None:
        """Callback that is called during heartbeat. This method can be overwritten by the runners."""

    @classmethod
    @provide_session
    def most_recent_job(cls, session: Session = NEW_SESSION) -> Job | None:
        """Returns the most recent job of this type, if any, based on last heartbeat received."""
        from airflow.jobs.job import most_recent_job

        return most_recent_job(cls.job_type, session=session)
