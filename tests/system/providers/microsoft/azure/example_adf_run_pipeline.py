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
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import cast

from airflow.models import DAG
from airflow.models.xcom_arg import XComArg

# Ignore missing args provided by default_args
# mypy: disable-error-code="call-arg"

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from airflow.utils.edgemodifier import Label

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_adf_run_pipeline"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 8, 13),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "azure_data_factory_conn_id": "azure_data_factory",
        "factory_name": "my-data-factory",  # This can also be specified in the ADF connection.
        "resource_group_name": "my-resource-group",  # This can also be specified in the ADF connection.
    },
    default_view="graph",
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    # [START howto_operator_adf_run_pipeline]
    run_pipeline1 = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline1",
        pipeline_name="pipeline1",
        parameters={"myParam": "value"},
    )
    # [END howto_operator_adf_run_pipeline]

    # [START howto_operator_adf_run_pipeline_async]
    run_pipeline2 = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline2",
        pipeline_name="pipeline2",
        wait_for_termination=False,
    )

    pipeline_run_sensor = AzureDataFactoryPipelineRunStatusSensor(
        task_id="pipeline_run_sensor",
        run_id=cast(str, XComArg(run_pipeline2, key="run_id")),
    )

    # Performs polling on the Airflow Triggerer thus freeing up resources on Airflow Worker
    pipeline_run_sensor_deferred = AzureDataFactoryPipelineRunStatusSensor(
        task_id="pipeline_run_sensor_defered",
        run_id=cast(str, XComArg(run_pipeline2, key="run_id")),
        deferrable=True,
    )

    pipeline_run_async_sensor = AzureDataFactoryPipelineRunStatusSensor(
        task_id="pipeline_run_async_sensor",
        run_id=cast(str, XComArg(run_pipeline2, key="run_id")),
        deferrable=True,
    )
    # [END howto_operator_adf_run_pipeline_async]

    # [START howto_operator_adf_run_pipeline_with_deferrable_flag]
    run_pipeline3 = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline3",
        pipeline_name="pipeline1",
        parameters={"myParam": "value"},
        deferrable=True,
    )
    # [END howto_operator_adf_run_pipeline_with_deferrable_flag]

    begin >> Label("No async wait") >> run_pipeline1
    begin >> Label("Do async wait with sensor") >> run_pipeline2
    begin >> Label("Do async wait with deferrable operator") >> run_pipeline3
    [
        run_pipeline1,
        pipeline_run_sensor,
        pipeline_run_sensor_deferred,
        pipeline_run_async_sensor,
        run_pipeline3,
    ] >> end
    [run_pipeline1, pipeline_run_sensor, pipeline_run_sensor_deferred, pipeline_run_async_sensor] >> end

    # Task dependency created via `XComArgs`:
    #   run_pipeline2 >> pipeline_run_sensor

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
