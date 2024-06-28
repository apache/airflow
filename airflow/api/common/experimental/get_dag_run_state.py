#
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
"""DAG run APIs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from deprecated import deprecated

from airflow.api.common.experimental import check_and_get_dag, check_and_get_dagrun
from airflow.exceptions import RemovedInAirflow3Warning

if TYPE_CHECKING:
    from datetime import datetime


@deprecated(reason="Use DagRun().get_state() instead", version="2.2.4", category=RemovedInAirflow3Warning)
def get_dag_run_state(dag_id: str, execution_date: datetime) -> dict[str, str]:
    """
    Return the Dag Run state identified by the given dag_id and execution_date.

    :param dag_id: DAG id
    :param execution_date: execution date
    :return: Dictionary storing state of the object
    """
    dag = check_and_get_dag(dag_id=dag_id)

    dagrun = check_and_get_dagrun(dag, execution_date)

    return {"state": dagrun.get_state()}
