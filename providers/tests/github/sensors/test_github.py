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
from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.github.sensors.github import GithubTagSensor
from airflow.utils import db, timezone

pytestmark = pytest.mark.db_test


DEFAULT_DATE = timezone.datetime(2017, 1, 1)
github_client_mock = Mock(name="github_client_for_test")


class TestGithubSensor:
    def setup_class(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG("test_dag_id", schedule=None, default_args=args)
        self.dag = dag
        db.merge_conn(
            Connection(
                conn_id="github_default",
                conn_type="github",
                password="my-access-token",
                host="https://mygithub.com/api/v3",
            )
        )

    @patch(
        "airflow.providers.github.hooks.github.GithubClient",
        autospec=True,
        return_value=github_client_mock,
    )
    def test_github_tag_created(self, github_mock):
        class MockTag:
            pass

        tag = MockTag()
        tag.name = "v1.0"

        github_mock.return_value.get_repo.return_value.get_tags.return_value = [tag]

        github_tag_sensor = GithubTagSensor(
            task_id="search-ticket-test",
            tag_name="v1.0",
            repository_name="pateash/jetbrains_settings",
            timeout=60,
            poke_interval=10,
            dag=self.dag,
        )

        github_tag_sensor.run(
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True
        )

        assert github_mock.return_value.get_repo.called
        assert github_mock.return_value.get_repo.return_value.get_tags.called
