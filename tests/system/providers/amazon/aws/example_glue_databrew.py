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

import pendulum

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.glue_databrew import (
    GlueDataBrewStartJobOperator,
)
from tests.system.providers.amazon.aws.utils import SystemTestContextBuilder

DAG_ID = "example_databrew"

sys_test_context_task = SystemTestContextBuilder().build()


with DAG(DAG_ID, schedule="@once", start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), catchup=False) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    job_name = f"{env_id}-databrew-job"

    # [START howto_operator_glue_databrew_start]
    start_job = GlueDataBrewStartJobOperator(task_id="startjob", deferrable=True, job_name=job_name, delay=15)
    # [END howto_operator_glue_databrew_start]

    chain(test_context, start_job)

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
