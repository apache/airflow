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
"""Refresh an existing data profiling monitor."""

from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.common.compat.sdk import DAG
from airflow.providers.databricks.operators.data_quality import DatabricksDataQualityRefreshOperator

TABLE_NAME = os.environ.get("DATA_QUALITY_TABLE_NAME", "catalog.schema.monitored_table")

with DAG(
    dag_id="example_databricks_data_quality",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["example"],
) as dag:
    # [START howto_operator_databricks_data_quality_refresh]
    refresh_metrics = DatabricksDataQualityRefreshOperator(
        task_id="refresh_metrics",
        table_name=TABLE_NAME,
        databricks_conn_id="databricks_default",
        timeout=1800,
        retries=0,
    )
    # [END howto_operator_databricks_data_quality_refresh]

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
