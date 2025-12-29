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

from airflow.models.taskmap import TaskMap

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.serialization.definitions.mappedoperator import Operator


def expand_mapped_task(
    mapped: Operator,
    run_id: str,
    upstream_task_id: str,
    length: int,
    session: Session,
):
    session.add(
        TaskMap(
            dag_id=mapped.dag_id,
            task_id=upstream_task_id,
            run_id=run_id,
            map_index=-1,
            length=length,
            keys=None,
        )
    )
    session.flush()

    TaskMap.expand_mapped_task(mapped, run_id, session=session)
