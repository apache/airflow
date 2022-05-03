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
import logging
from datetime import datetime
from typing import Any, Optional

from github import GithubException

from airflow import AirflowException
from airflow.models.dag import DAG
from airflow.providers.github.operators.github import GithubOperator
from airflow.providers.github.sensors.github import GithubSensor, GithubTagSensor

dag = DAG(
    'example_github_operator',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
)

# [START howto_tag_sensor_github]

tag_sensor = GithubTagSensor(
    task_id='example_tag_sensor',
    tag_name='v1.0',
    repository_name="apache/airflow",
    timeout=60,
    poke_interval=10,
    dag=dag,
)


# [END howto_tag_sensor_github]

# [START howto_sensor_github]


def tag_checker(repo: Any, tag_name: str) -> Optional[bool]:
    result = None
    try:
        if repo is not None and tag_name is not None:
            all_tags = [x.name for x in repo.get_tags()]
            result = tag_name in all_tags

    except GithubException as github_error:  # type: ignore[misc]
        raise AirflowException(f"Failed to execute GithubSensor, error: {str(github_error)}")
    except Exception as e:
        raise AirflowException(f"Github operator error: {str(e)}")
    return result


github_sensor = GithubSensor(
    task_id='example_sensor',
    method_name="get_repo",
    method_params={'full_name_or_id': "apache/airflow"},
    result_processor=lambda repo: tag_checker(repo, 'v1.0'),
    timeout=60,
    poke_interval=10,
    dag=dag,
)

# [END howto_sensor_github]


# [START howto_operator_list_repos_github]

github_list_repos = GithubOperator(
    task_id='github_list_repos',
    github_method="get_user",
    github_method_args={},
    result_processor=lambda user: logging.info(list(user.get_repos())),
    dag=dag,
)

# [END howto_operator_list_repos_github]

# [START howto_operator_list_tags_github]

list_repo_tags = GithubOperator(
    task_id='list_repo_tags',
    github_method="get_repo",
    github_method_args={'full_name_or_id': 'apache/airflow'},
    result_processor=lambda repo: logging.info(list(repo.get_tags())),
    dag=dag,
)

# [END howto_operator_list_tags_github]
