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
This is an example dag for using the AzureContainerInstancesOperator.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.microsoft.azure.operators.container_instances import AzureContainerInstancesOperator

with DAG(
    dag_id='aci_example',
    default_args={'retries': 1},
    schedule_interval=timedelta(days=1),
    start_date=datetime(2018, 11, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = AzureContainerInstancesOperator(
        ci_conn_id='azure_default',
        registry_conn_id=None,
        resource_group='resource-group',
        name='aci-test-{{ ds }}',
        image='hello-world',
        region='WestUS2',
        environment_variables={},
        volumes=[],
        memory_in_gb=4.0,
        cpu=1.0,
        task_id='start_container',
    )
