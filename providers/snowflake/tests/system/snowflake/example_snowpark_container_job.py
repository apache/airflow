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
Example use of SnowparkContainerJobOperator.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowpark_containers import SnowparkContainerJobOperator

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
DAG_ID = "example_snowpark_container_job"

with DAG(
    DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_operator_snowpark_container_job]
    run_job = SnowparkContainerJobOperator(
        task_id="run_job",
        compute_pool="my_compute_pool",
        container_name="main",
        spec="spec.yaml",
        spec_stage="@my_stage",
    )
    # [END howto_operator_snowpark_container_job]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
