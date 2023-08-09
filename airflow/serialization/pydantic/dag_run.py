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
from typing import List, Optional

from pydantic import BaseModel as BaseModelPydantic

from airflow.serialization.pydantic.dataset import DatasetEventPydantic


class DagRunPydantic(BaseModelPydantic):
    """Serializable representation of the DagRun ORM SqlAlchemyModel used by internal API."""

    id: int
    dag_id: str
    queued_at: Optional[datetime]
    execution_date: datetime
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    state: str
    run_id: str
    creating_job_id: Optional[int]
    external_trigger: bool
    run_type: str
    data_interval_start: Optional[datetime]
    data_interval_end: Optional[datetime]
    last_scheduling_decision: Optional[datetime]
    dag_hash: Optional[str]
    updated_at: datetime
    consumed_dataset_events: List[DatasetEventPydantic]

    class Config:
        """Make sure it deals automatically with SQLAlchemy ORM classes."""

        from_attributes = True
        orm_mode = True  # Pydantic 1.x compatibility.
