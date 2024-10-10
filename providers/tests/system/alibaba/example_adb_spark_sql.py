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
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.alibaba.cloud.operators.analyticdb_spark import AnalyticDBSparkSQLOperator

# Ignore missing args provided by default_args
# mypy: disable-error-code="call-arg"

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "adb_spark_sql_dag"
# [START howto_operator_adb_spark_sql]
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    default_args={"cluster_id": "your cluster", "rg_name": "your resource group", "region": "your region"},
    max_active_runs=1,
    catchup=False,
) as dag:
    show_databases = AnalyticDBSparkSQLOperator(task_id="task1", sql="SHOE DATABASES;")

    show_tables = AnalyticDBSparkSQLOperator(task_id="task2", sql="SHOW TABLES;")

    show_databases >> show_tables

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()
# [END howto_operator_adb_spark_sql]

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
