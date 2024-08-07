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

import os
from typing import List

from airflow.exceptions import AirflowException
from airflow.models import DagModel, DagBag
from airflow.utils.file import correct_maybe_zipped
from airflow.utils.session import NEW_SESSION


def check_for_existence(external_dag_id: str = None,
                        external_task_ids: List[str] = None,
                        external_task_group_id: str = None,
                        session=NEW_SESSION
                        ) -> None:

    dag_to_wait = DagModel.get_current(dag_id=external_dag_id,
                                       session=session)

    if not dag_to_wait:
        raise AirflowException(f"The external DAG {external_dag_id} does not exist.")

    if not os.path.exists(correct_maybe_zipped(dag_to_wait.fileloc)):
        raise AirflowException(f"The external DAG {external_dag_id} was deleted.")

    if external_task_ids:
        refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(external_dag_id)
        for external_task_id in external_task_ids:
            if not refreshed_dag_info.has_task(external_task_id):
                raise AirflowException(
                    f"The external task {external_task_id} in "
                    f"DAG {external_dag_id} does not exist."
                )

    if external_task_group_id:
        refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(external_dag_id)
        if not refreshed_dag_info.has_task_group(external_task_group_id):
            raise AirflowException(
                f"The external task group '{external_task_group_id}' in "
                f"DAG '{external_dag_id}' does not exist."
            )
