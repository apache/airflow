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

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.microsoft.azure.operators.azure_data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.azure_data_factory import (
    AzureDataFactoryPipelineRunStatusSensor,
)
from airflow.utils.edgemodifier import Label

with DAG(
    dag_id="example_adf_run_pipeline",
    start_date=datetime(2021, 8, 13),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "conn_id": "azure_data_factory",
        "factory_name": "my-data-factory",  # This can also be specified in the ADF connection.
        "resource_group_name": "my-resource-group",  # This can also be specified in the ADF connection.
    },
    default_view="graph",
) as dag:
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    # [START howto_operator_adf_run_pipeline]
    run_pipeline1 = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline1",
        pipeline_name="pipeline1",
        parameters={"myParam": "value"},
    )
    # [END howto_operator_adf_run_pipeline]

    run_pipeline2 = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline2",
        pipeline_name="run_pipeline2",
        wait_for_completion=False,
    )

    wait_for_pipeline_run = AzureDataFactoryPipelineRunStatusSensor(
        task_id="wait_for_pipeline_run",
        run_id=run_pipeline2.output["run_id"],
        poke_interval=10,
    )

    begin >> Label("No async wait") >> run_pipeline1 >> end
    begin >> Label("Do async wait with sensor") >> run_pipeline2
    wait_for_pipeline_run >> end

    # Task dependency created via `XComArgs`:
    #   run_pipeline2 >> wait_for_pipeline_run
