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

import logging
import os
from datetime import datetime
from typing import Any

from github import GithubException

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.providers.github.operators.github import GithubOperator
from airflow.providers.github.sensors.github import GithubSensor, GithubTagSensor

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_github_operator"

logger = logging.getLogger(__name__)

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_tag_sensor_github]

    tag_sensor = GithubTagSensor(
        task_id="example_tag_sensor",
        tag_name="v1.0",
        repository_name="apache/airflow",
        timeout=60,
        poke_interval=10,
    )

    # [END howto_tag_sensor_github]

    # [START howto_sensor_github]

    def tag_checker(repo: Any, tag_name: str) -> bool | None:
        result = None
        try:
            if repo is not None and tag_name is not None:
                all_tags = [x.name for x in repo.get_tags()]
                result = tag_name in all_tags

        except GithubException as github_error:  # type: ignore[misc]
            raise AirflowException(
                f"Failed to execute GithubSensor, error: {github_error}"
            )
        except Exception as e:
            raise AirflowException(f"GitHub operator error: {e}")
        return result

    github_sensor = GithubSensor(
        task_id="example_sensor",
        method_name="get_repo",
        method_params={"full_name_or_id": "apache/airflow"},
        result_processor=lambda repo: tag_checker(repo, "v1.0"),
        timeout=60,
        poke_interval=10,
    )

    # [END howto_sensor_github]

    # [START howto_operator_list_repos_github]

    github_list_repos = GithubOperator(
        task_id="github_list_repos",
        github_method="get_user",
        result_processor=lambda user: logger.info(list(user.get_repos())),
    )

    # [END howto_operator_list_repos_github]

    # [START howto_operator_list_tags_github]

    list_repo_tags = GithubOperator(
        task_id="list_repo_tags",
        github_method="get_repo",
        github_method_args={"full_name_or_id": "apache/airflow"},
        result_processor=lambda repo: logger.info(list(repo.get_tags())),
    )

    # [END howto_operator_list_tags_github]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
