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

"""Tests for Cloud SQL links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.cloud_sql import (
    CLOUD_SQL_INSTANCE_DATABASE_LINK,
    CLOUD_SQL_INSTANCE_LINK,
    CloudSQLInstanceDatabaseLink,
    CloudSQLInstanceLink,
)

TEST_INSTANCE = "test-instance"
TEST_PROJECT_ID = "test-project-id"


class TestCloudSQLInstanceLink:
    def test_class_attributes(self):
        assert CloudSQLInstanceLink.key == "cloud_sql_instance"
        assert CloudSQLInstanceLink.name == "Cloud SQL Instance"
        assert CloudSQLInstanceLink.format_str == CLOUD_SQL_INSTANCE_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudSQLInstanceLink.persist(
            context=mock_context,
            instance=TEST_INSTANCE,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_sql_instance",
            value={"instance": TEST_INSTANCE, "project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = CloudSQLInstanceLink()

        result = link._format_link(instance=TEST_INSTANCE, project_id=TEST_PROJECT_ID)

        assert result == BASE_LINK + CLOUD_SQL_INSTANCE_LINK.format(
            instance=TEST_INSTANCE, project_id=TEST_PROJECT_ID
        )


class TestCloudSQLInstanceDatabaseLink:
    def test_class_attributes(self):
        assert CloudSQLInstanceDatabaseLink.key == "cloud_sql_instance_database"
        assert CloudSQLInstanceDatabaseLink.name == "Cloud SQL Instance Database"
        assert CloudSQLInstanceDatabaseLink.format_str == CLOUD_SQL_INSTANCE_DATABASE_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudSQLInstanceDatabaseLink.persist(
            context=mock_context,
            instance=TEST_INSTANCE,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_sql_instance_database",
            value={"instance": TEST_INSTANCE, "project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = CloudSQLInstanceDatabaseLink()

        result = link._format_link(instance=TEST_INSTANCE, project_id=TEST_PROJECT_ID)

        assert result == BASE_LINK + CLOUD_SQL_INSTANCE_DATABASE_LINK.format(
            instance=TEST_INSTANCE, project_id=TEST_PROJECT_ID
        )
