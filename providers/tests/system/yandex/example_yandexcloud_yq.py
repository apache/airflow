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

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.yandex.operators.yq import YQExecuteQueryOperator

from tests_common.test_utils.system_tests import get_test_env_id

ENV_ID = get_test_env_id()
DAG_ID = "example_yandexcloud_yq"

with DAG(
    DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["example"],
) as dag:
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    yq_operator = YQExecuteQueryOperator(
        task_id="sample_query", sql="select 33 as d, 44 as t"
    )
    yq_operator >> run_this_last

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
