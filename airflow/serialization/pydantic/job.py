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
from datetime import datetime
from typing import Optional

from pydantic import BaseModel as BaseModelPydantic

from airflow.jobs.base_job_runner import BaseJobRunner


def check_runner_initialized(job_runner: Optional[BaseJobRunner], job_type: str) -> BaseJobRunner:
    if job_runner is None:
        raise ValueError(f"In order to run {job_type} you need to initialize the {job_type}Runner first.")
    return job_runner


class JobPydantic(BaseModelPydantic):
    """Serializable representation of the Job ORM SqlAlchemyModel used by internal API."""

    id: Optional[int]
    dag_id: Optional[str]
    state: Optional[str]
    job_type: Optional[str]
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    latest_heartbeat: datetime
    executor_class: Optional[str]
    hostname: Optional[str]
    unixname: Optional[str]

    # not an ORM field
    heartrate: Optional[int]
    max_tis_per_query: Optional[int]

    class Config:
        """Make sure it deals automatically with SQLAlchemy ORM classes."""

        from_attributes = True
        orm_mode = True  # Pydantic 1.x compatibility.
