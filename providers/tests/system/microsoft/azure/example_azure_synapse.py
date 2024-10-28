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

from airflow import DAG
from airflow.providers.microsoft.azure.operators.synapse import (
    AzureSynapseRunSparkBatchOperator,
)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

SPARK_JOB_PAYLOAD = {
    "name": "SparkJob",
    "file": "abfss://spark@providersstorageaccgen2.dfs.core.windows.net/wordcount.py",
    "args": [
        "abfss://spark@providersstorageaccgen2.dfs.core.windows.net/shakespeare.txt",
        "abfss://spark@providersstorageaccgen2.dfs.core.windows.net/results/",
    ],
    "jars": [],
    "pyFiles": [],
    "files": [],
    "conf": {
        "spark.dynamicAllocation.enabled": "false",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "2",
    },
    "numExecutors": 2,
    "executorCores": 4,
    "executorMemory": "28g",
    "driverCores": 4,
    "driverMemory": "28g",
}

with DAG(
    dag_id="example_synapse_spark_job",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "synapse"],
) as dag:
    # [START howto_operator_azure_synapse]
    run_spark_job = AzureSynapseRunSparkBatchOperator(
        task_id="run_spark_job",
        spark_pool="provsparkpool",
        payload=SPARK_JOB_PAYLOAD,  # type: ignore
    )
    # [END howto_operator_azure_synapse]

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
