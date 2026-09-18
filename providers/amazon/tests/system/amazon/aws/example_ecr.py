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

from airflow.providers.amazon.aws.operators.ecr import (
    EcrCreateRepositoryOperator,
    EcrDeleteRepositoryOperator,
    EcrSetRepositoryPolicyOperator,
)
from airflow.providers.common.compat.sdk import DAG, chain

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import TriggerRule
else:
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "example_ecr"

sys_test_context_task = SystemTestContextBuilder().build()


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    repository_name = f"{test_context[ENV_ID_KEY]}-test-repository"

    # [START howto_operator_ecr_create_repository]
    create_repository = EcrCreateRepositoryOperator(
        task_id="create_repository",
        repository_name=repository_name,
    )
    # [END howto_operator_ecr_create_repository]

    # [START howto_operator_ecr_set_repository_policy]
    set_repository_policy = EcrSetRepositoryPolicyOperator(
        task_id="set_repository_policy",
        repository_name=repository_name,
        policy_text="""
        {
          "Version": "2012-10-17",
          "Statement": [
            {
              "Sid": "AllowAccountPull",
              "Effect": "Allow",
              "Principal": {
                "AWS": "arn:aws:iam::{{ task_instance.xcom_pull(task_ids='create_repository')['repository']['registryId'] }}:root"
              },
              "Action": [
                "ecr:BatchGetImage",
                "ecr:GetDownloadUrlForLayer"
              ]
            }
          ]
        }
        """,
    )
    # [END howto_operator_ecr_set_repository_policy]

    # [START howto_operator_ecr_delete_repository]
    delete_repository = EcrDeleteRepositoryOperator(
        task_id="delete_repository",
        repository_name=repository_name,
        force=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_ecr_delete_repository]

    chain(test_context, create_repository, set_repository_policy, delete_repository)

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
