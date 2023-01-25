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
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.neptune import NeptuneStartDbOperator, NeptuneStopDbOperator
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "example_neptune_cluster"

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2023, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    # Assuming Neptune DB is already created, its identifier is provided to test NeptuneStartDbOperator
    # and NeptuneStopDbOperator
    neptune_db_identifier = f"{test_context[ENV_ID_KEY]}-neptune-database"

    # [START howto_operator_neptune_start_db]
    stop_db_instance = NeptuneStartDbOperator(
        task_id="stop_db_instance",
        db_identifier=neptune_db_identifier,
    )
    # [END howto_operator_neptune_start_db]

    # [START howto_operator_neptune_stop_db]
    start_db_instance = NeptuneStopDbOperator(
        task_id="start_db_instance",
        db_identifier=neptune_db_identifier,
    )
    # [END howto_operator_neptune_stop_db]

    chain(
        # TEST SETUP
        test_context,
        # TEST BODY
        start_db_instance,
        stop_db_instance,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
