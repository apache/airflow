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

from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic


class BaseJobPydantic(BaseModelPydantic):
    """Serializable representation of the BaseJob ORM SqlAlchemyModel used by internal API"""

    id: Optional[int]
    dag_id: Optional[str]
    state: Optional[str]
    job_type: Optional[str]
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    latest_heartbeat: Optional[datetime]
    executor_class: Optional[str]
    hostname: Optional[str]
    unixname: Optional[str]
    task_instance: TaskInstancePydantic

    class Config:
        """Make sure it deals automatically with ORM classes of SQL Alchemy"""

        orm_mode = True
