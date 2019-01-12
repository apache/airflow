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

"""
This is an example DAG to highlight usage of the AzureBatchAIOperator to run a
Batch AI Cluster on Azure
"""

import airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.azure_batchai_operator import AzureBatchAIOperator
from airflow.models import DAG


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='example_azure_batchai_operator',
    default_args=args,
    schedule_interval="@daily")

batch_ai_node = AzureBatchAIOperator(
    bai_conn_id='azure_batchai_default',
    resource_group='batch-ai-test-rg',
    workspace_name='batch-ai-workspace-name',
    cluster_name='batch-ai-cluster-name',
    location='WestUS2',
    environment_variables={},
    volumes=[],
    task_id='run_this_first',
    dag=dag)

dummy = DummyOperator(
    task_id='join',
    dag=dag
)

batch_ai_node >> dummy
