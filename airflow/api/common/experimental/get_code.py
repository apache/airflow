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
"""Get code APIs."""
from __future__ import annotations

from deprecated import deprecated

from airflow.api.common.experimental import check_and_get_dag
from airflow.exceptions import AirflowException, DagCodeNotFound
from airflow.models.dagcode import DagCode


@deprecated(reason="Use DagCode().get_code_by_fileloc() instead", version="2.2.4")
def get_code(dag_id: str) -> str:
    """Return python code of a given dag_id.

    :param dag_id: DAG id
    :return: code of the DAG
    """
    dag = check_and_get_dag(dag_id=dag_id)

    try:
        return DagCode.get_code_by_fileloc(dag.fileloc)
    except (OSError, DagCodeNotFound) as exception:
        error_message = f"Error {str(exception)} while reading Dag id {dag_id} Code"
        raise AirflowException(error_message, exception)
