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

from sqlalchemy.orm import joinedload
from sqlalchemy.orm.interfaces import LoaderOption

from airflow.models import Base
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance


def eager_load_TI_and_TIH_for_validation(orm_model: Base | None = None) -> tuple[LoaderOption, ...]:
    """Construct the eager loading options necessary for both TaskInstanceResponse and TaskInstanceHistoryResponse objects."""
    if orm_model is None:
        orm_model = TaskInstance

    options: tuple[LoaderOption, ...] = (
        joinedload(orm_model.dag_version).joinedload(DagVersion.bundle),
        joinedload(orm_model.dag_run).options(joinedload(DagRun.dag_model)),
    )
    if orm_model is TaskInstance:
        options += (joinedload(orm_model.task_instance_note),)
    return options
