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

import os
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator
from airflow.providers.amazon.aws.sensors.emr import EmrContainerSensor
from tests.system.providers.amazon.aws.utils import set_env_id

ENV_ID = set_env_id()
DAG_ID = 'example_emr_eks'
VIRTUAL_CLUSTER_ID = os.getenv("VIRTUAL_CLUSTER_ID", f"{ENV_ID}-test-cluster")
JOB_ROLE_ARN = os.getenv("JOB_ROLE_ARN", "arn:aws:iam::012345678912:role/emr_eks_default_role")

# [START howto_operator_emr_eks_config]
JOB_DRIVER_ARG = {
    "sparkSubmitJobDriver": {
        "entryPoint": "local:///usr/lib/spark/examples/src/main/python/pi.py",
        "sparkSubmitParameters": "--conf spark.executors.instances=2 --conf spark.executors.memory=2G --conf spark.executor.cores=2 --conf spark.driver.cores=1",  # noqa: E501
    }
}

CONFIGURATION_OVERRIDES_ARG = {
    "applicationConfiguration": [
        {
            "classification": "spark-defaults",
            "properties": {
                "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",  # noqa: E501
            },
        }
    ],
    "monitoringConfiguration": {
        "cloudWatchMonitoringConfiguration": {
            "logGroupName": "/aws/emr-eks-spark",
            "logStreamNamePrefix": "airflow",
        }
    },
}
# [END howto_operator_emr_eks_config]

with DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    # [START howto_operator_emr_container]
    job_starter = EmrContainerOperator(
        task_id="start_job",
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        execution_role_arn=JOB_ROLE_ARN,
        release_label="emr-6.3.0-latest",
        job_driver=JOB_DRIVER_ARG,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        name="pi.py",
        wait_for_completion=False,
    )
    # [END howto_operator_emr_container]

    # [START howto_sensor_emr_container]
    job_waiter = EmrContainerSensor(
        task_id="job_waiter", virtual_cluster_id=VIRTUAL_CLUSTER_ID, job_id=str(job_starter.output)
    )
    # [END howto_sensor_emr_container]

    chain(job_starter, job_waiter)

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
