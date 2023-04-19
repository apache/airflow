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
from typing import Any, Iterable, Optional, Union

from pydantic import BaseModel as BaseModelPydantic

from airflow.serialization.pydantic.dag_run import DagRunPydantic
from airflow.utils.xcom import XCOM_RETURN_KEY


class TaskInstancePydantic(BaseModelPydantic):
    """Serializable representation of the TaskInstance ORM SqlAlchemyModel used by internal API"""

    task_id: str
    dag_id: str
    run_id: str
    map_index: int
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    execution_date: Optional[datetime]
    duration: Optional[float]
    state: Optional[str]
    try_number: int
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

    def xcom_pull(
        self,
        task_ids: Optional[Union[str, Iterable[str]]] = None,
        dag_id: Optional[str] = None,
        key: str = XCOM_RETURN_KEY,
        include_prior_dates: bool = False,
        *,
        map_indexes: Optional[Union[int, Iterable[int]]] = None,
        default: Any = None,
    ) -> Any:
        """
        Pull an XCom value for this task instance.

        TODO: make it works for AIP-44
        :param task_ids: task id or list of task ids, if None, the task_id of the current task is used
        :param dag_id: dag id, if None, the dag_id of the current task is used
        :param key: the key to identify the XCom value
        :param include_prior_dates: whether to include prior execution dates
        :param map_indexes: map index or list of map indexes, if None, the map_index of the current task
            is used
        :param default: the default value to return if the XCom value does not exist
        :return: Xcom value
        """
        return None

    def xcom_push(
        self,
        key: str,
        value: Any,
        execution_date: Optional[datetime] = None,
    ) -> None:
        """
        Push an XCom value for this task instance.

        TODO: make it works for AIP-44
        :param key: the key to identify the XCom value
        :param value: the value of the XCom
        :param execution_date: the execution date to push the XCom for
        """
        pass

    def get_dagrun(self) -> DagRunPydantic:
        """
        Get the DagRun for this task instance.

        TODO: make it works for AIP-44

        :return: Pydantic serialized version of DaGrun
        """
        raise NotImplementedError()
