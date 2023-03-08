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


class TaskInstancePydantic(BaseModelPydantic):
    """Serializable representation of the TaskInstance ORM SqlAlchemyModel used by internal API"""

    task_id: str
    dag_id: str
    run_id: str
    map_index: str
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    duration: Optional[float]
    state: Optional[str]
    _try_number: int
    max_tries: int
    hostname: str
    unixname: str
    job_id: Optional[int]
    pool: str
    pool_slots: int
    queue: str
    priority_weight: Optional[int]
    operator: str
    queued_dttm: Optional[str]
    queued_by_job_id: Optional[int]
    pid: Optional[int]
    updated_at: Optional[datetime]
    external_executor_id: Optional[str]
    trigger_id: Optional[int]
    trigger_timeout: Optional[datetime]
    next_method: Optional[str]
    next_kwargs: Optional[dict]
    run_as_user: Optional[str]

    class Config:
        """Make sure it deals automatically with ORM classes of SQL Alchemy"""

        orm_mode = True
