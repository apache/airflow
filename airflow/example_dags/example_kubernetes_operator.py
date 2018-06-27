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

import airflow
import logging
from airflow.models import DAG

try:
    # Kubernetes is optional, so not available in vanilla Airflow
    # pip install airflow[gcp]
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
except ImportError:
    # Just import the BaseOperator as the KubernetesPodOperator
    logging.warn("Could not import KubernetesPodOperator")
    from airflow.models import BaseOperator as KubernetesPodOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='example_kubernetes_operator',
    default_args=args,
    schedule_interval=None)

k = KubernetesPodOperator(
    namespace='default',
    image="ubuntu:16.04",
    cmds=["bash", "-cx"],
    arguments=["echo", "10"],
    labels={"foo": "bar"},
    name="airflow-test-pod",
    in_cluster=False,
    task_id="task",
    get_logs=True,
    dag=dag)
