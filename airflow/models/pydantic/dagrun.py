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

from datetime import datetime

from pydantic import BaseModel as BaseModelPydantic


class DagRunPydantic(BaseModelPydantic):
    """Serializable representation of the DagRun ORM SqlAlchemyModel used by internal API."""

    id: int
    dag_id: str
    queued_at: datetime | None
    execution_date: datetime
    start_date: datetime | None
    end_date: datetime | None
    state: str
    run_id: str | None
    creating_job_id: int | None
    external_trigger: bool
    run_type: str
    data_interval_start: datetime | None
    data_interval_end: datetime | None
    last_scheduling_decision: datetime | None
    dag_hash: str | None
    updated_at: datetime

    class Config:
        """Make sure it deals automatically with ORM classes of SQL Alchemy"""

        orm_mode = True
