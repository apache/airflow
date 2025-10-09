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
Example Airflow DAG to submit Apache Druid json index file using `DruidOperator`
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models import DAG
from airflow.providers.apache.druid.operators.druid import DruidOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_druid_operator"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # [START howto_operator_druid_submit]
    submit_job = DruidOperator(task_id="spark_submit_job", json_index_file="json_index.json")
    # Example content of json_index.json:
    JSON_INDEX_STR = """
        {
            "type": "index_hadoop",
            "datasource": "datasource_prd",
            "spec": {
                "dataSchema": {
                    "granularitySpec": {
                        "intervals": ["2021-09-01/2021-09-02"]
                    }
                }
            }
        }
    """
    # [END howto_operator_druid_submit]

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
