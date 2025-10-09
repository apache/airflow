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

from datetime import datetime

from airflow import DAG
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreateSparkJobOperator,
    DataprocDeleteClusterOperator,
)

# Name of the datacenter where Dataproc cluster will be created
try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from tests_common.test_utils.system_tests import get_test_env_id

# should be filled with appropriate ids


AVAILABILITY_ZONE_ID = "ru-central1-c"

# Dataproc cluster will use this bucket as distributed storage
S3_BUCKET_NAME = ""

ENV_ID = get_test_env_id()
DAG_ID = "example_yandexcloud_dataproc_lightweight"

with DAG(
    DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["example"],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        zone=AVAILABILITY_ZONE_ID,
        s3_bucket=S3_BUCKET_NAME,
        computenode_count=1,
        datanode_count=0,
        services=("SPARK", "YARN"),
    )

    create_spark_job = DataprocCreateSparkJobOperator(
        cluster_id=create_cluster.cluster_id,
        task_id="create_spark_job",
        main_jar_file_uri="file:///usr/lib/spark/examples/jars/spark-examples.jar",
        main_class="org.apache.spark.examples.SparkPi",
        args=["1000"],
    )

    delete_cluster = DataprocDeleteClusterOperator(
        cluster_id=create_cluster.cluster_id,
        task_id="delete_cluster",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    create_spark_job >> delete_cluster

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
