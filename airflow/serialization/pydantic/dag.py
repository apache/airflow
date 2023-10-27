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
from typing import List, Optional

from pydantic import BaseModel as BaseModelPydantic

from airflow.configuration import conf as airflow_conf


class DagOwnerAttributesPydantic(BaseModelPydantic):
    """Serializable representation of the DagOwnerAttributes ORM SqlAlchemyModel used by internal API."""

    owner: str
    link: str

    class Config:
        """Make sure it deals automatically with SQLAlchemy ORM classes."""

        from_attributes = True
        orm_mode = True  # Pydantic 1.x compatibility.
        arbitrary_types_allowed = True


class DagTagPydantic(BaseModelPydantic):
    """Serializable representation of the DagTag ORM SqlAlchemyModel used by internal API."""

    name: str
    dag_id: str

    class Config:
        """Make sure it deals automatically with SQLAlchemy ORM classes."""

        from_attributes = True
        orm_mode = True  # Pydantic 1.x compatibility.
        arbitrary_types_allowed = True


class DagModelPydantic(BaseModelPydantic):
    """Serializable representation of the DagModel ORM SqlAlchemyModel used by internal API."""

    dag_id: str
    root_dag_id: Optional[str]
    is_paused_at_creation: bool = airflow_conf.getboolean("core", "dags_are_paused_at_creation")
    is_paused: bool = is_paused_at_creation
    is_subdag: Optional[bool] = False
    is_active: Optional[bool] = False
    last_parsed_time: Optional[datetime]
    last_pickled: Optional[datetime]
    last_expired: Optional[datetime]
    scheduler_lock: Optional[bool]
    pickle_id: Optional[int]
    fileloc: str
    processor_subdir: Optional[str]
    owners: Optional[str]
    description: Optional[str]
    default_view: Optional[str]
    schedule_interval: Optional[str]
    timetable_description: Optional[str]
    tags: List[DagTagPydantic]  # noqa
    dag_owner_links: List[DagOwnerAttributesPydantic]  # noqa

    max_active_tasks: int
    max_active_runs: Optional[int]

    has_task_concurrency_limits: bool
    has_import_errors: Optional[bool] = False

    class Config:
        """Make sure it deals automatically with SQLAlchemy ORM classes."""

        from_attributes = True
        orm_mode = True  # Pydantic 1.x compatibility.
        arbitrary_types_allowed = True
