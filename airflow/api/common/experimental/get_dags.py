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

from airflow.models import DagBag
from airflow import configuration


def get_dags(is_paused=None):
    """
    Returns a list of all Dags
    :return: List of all DAGs
    """
    dagbag = DagBag()
    dag_list = list()
    if is_paused:
        is_paused = is_paused.lower()
        is_paused = 1 if is_paused == 'true' else 0

    for dag_id in dagbag.dags:
        dag = dagbag.get_dag(dag_id)
        dag_list.append({
            'dag_id': dag.dag_id,
            'parent_dag_id': dag.parent_dag.dag_id if dag.is_subdag else None,
            'is_paused': dag.is_paused,
            'default_view': dag.get_default_view(),
            'owners': dag.owner,
            'pickle_id': dag.pickle_id,
            'description': dag.description,
            'fileloc': dag.fileloc,
            'is_subdag': dag.is_subdag,
            'schedule_interval': str(dag.schedule_interval),
            'task_count': dag.task_count,
            'task_ids': dag.task_ids,
            'resources': {
                'dag_runs': configuration.conf.get('webserver', 'BASE_URL') +
                '/api/experimental/dags/' +
                dag.dag_id +
                '/dag_runs'}})

    filtered_dag_list = dag_list
    if is_paused is not None:
        filtered_dag_list = [dag_object for dag_object in dag_list if dag['is_paused'] == is_paused]

    return filtered_dag_list
