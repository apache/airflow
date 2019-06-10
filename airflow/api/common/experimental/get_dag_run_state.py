# -*- coding: utf-8 -*-
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
from datetime import datetime
from typing import Dict

from airflow.api.common.experimental import check_and_get_dag, check_and_get_dagrun


def get_dag_run_state(dag_id, execution_date):  # type: (str, datetime) -> Dict[str, str]
    """Return the task object identified by the given dag_id and task_id.

    :param dag_id: DAG id
    :param execution_date: execution date
    :return: Dictionary storing state of the object
    """

    dag = check_and_get_dag(dag_id=dag_id)

    dagrun = check_and_get_dagrun(dag, execution_date)

    return {'state': dagrun.get_state()}
