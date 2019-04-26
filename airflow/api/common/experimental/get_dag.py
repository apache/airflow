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

from airflow.exceptions import AirflowException
from airflow.api.common.experimental.get_dags import get_dags


def get_dag(dag_id):
    """
    Returns DAG info for a specific DAG ID.
    :param dag_id: String identifier of a DAG
    :return: DAG info for a specific DAG ID.
    """
    dag_list = get_dags()

    for dag in dag_list:
        print(dag['dag_id'] + ' : ' + dag_id)
        if dag['dag_id'] == dag_id:
            return dag

    error_message = "Dag id {} not found".format(dag_id)
    raise AirflowException(error_message)
