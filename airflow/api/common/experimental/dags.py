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

from airflow.models import DagModel
from airflow.exceptions import DagNotFound
from flask import url_for


def get_dag_as_dict(dag):
    dag_dict = {'dag_id': dag.dag_id,
                'is_paused': dag.is_paused,
                'is_subdag': dag.is_subdag,
                'is_active': dag.is_active,
                'last_scheduler_run': dag.last_scheduler_run,
                'last_pickled': dag.last_pickled,
                'last_expired': dag.last_expired,
                'pickle_id': dag.pickle_id,
                'fileloc': dag.fileloc,
                'owners': dag.owners,
                'resources': {'dag_runs': url_for('api_experimental.dag_runs', dag_id=dag.dag_id)}}
    return dag_dict


def get_dags(is_paused=None, is_subdag=None, is_active=None, scheduler_lock=None):
    """
    Returns a list of all Dags
    :return: List of all DAGs
    """
    dag_list = list()

    for dag in DagModel.find(is_paused=is_paused, is_subdag=is_subdag, is_active=is_active,
                             scheduler_lock=scheduler_lock):
        dag_list.append(get_dag_as_dict(dag))

    return dag_list


def get_dag(dag_id):
    """
    Returns DAG info for a specific DAG ID.

    :param dag_id: String identifier of a DAG
    :return: DAG info for a specific DAG ID.
    """
    dag = DagModel.get_dagmodel(dag_id)
    if dag is not None:
        return get_dag_as_dict(dag)

    error_message = "Dag id {} not found".format(dag_id)
    raise DagNotFound(error_message)
