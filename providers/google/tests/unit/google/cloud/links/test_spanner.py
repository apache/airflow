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

"""Tests for Spanner links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.spanner import (
    SPANNER_DATABASE_LINK,
    SPANNER_INSTANCE_LINK,
    SpannerDatabaseLink,
    SpannerInstanceLink,
)

TEST_DATABASE_ID = "test-database-id"
TEST_INSTANCE_ID = "test-instance-id"
TEST_PROJECT_ID = "test-project-id"


class TestSpannerInstanceLink:
    def test_class_attributes(self):
        assert SpannerInstanceLink.key == "spanner_instance"
        assert SpannerInstanceLink.name == "Spanner Instance"
        assert SpannerInstanceLink.format_str == SPANNER_INSTANCE_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        SpannerInstanceLink.persist(
            context=mock_context,
            instance_id=TEST_INSTANCE_ID,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="spanner_instance",
            value={"instance_id": TEST_INSTANCE_ID, "project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = SpannerInstanceLink()

        result = link._format_link(instance_id=TEST_INSTANCE_ID, project_id=TEST_PROJECT_ID)

        assert result == BASE_LINK + SPANNER_INSTANCE_LINK.format(
            instance_id=TEST_INSTANCE_ID, project_id=TEST_PROJECT_ID
        )


class TestSpannerDatabaseLink:
    def test_class_attributes(self):
        assert SpannerDatabaseLink.key == "spanner_database"
        assert SpannerDatabaseLink.name == "Spanner Database"
        assert SpannerDatabaseLink.format_str == SPANNER_DATABASE_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        SpannerDatabaseLink.persist(
            context=mock_context,
            database_id=TEST_DATABASE_ID,
            instance_id=TEST_INSTANCE_ID,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="spanner_database",
            value={
                "database_id": TEST_DATABASE_ID,
                "instance_id": TEST_INSTANCE_ID,
                "project_id": TEST_PROJECT_ID,
            },
        )

    def test_format_link(self):
        link = SpannerDatabaseLink()

        result = link._format_link(
            database_id=TEST_DATABASE_ID, instance_id=TEST_INSTANCE_ID, project_id=TEST_PROJECT_ID
        )

        assert result == BASE_LINK + SPANNER_DATABASE_LINK.format(
            database_id=TEST_DATABASE_ID, instance_id=TEST_INSTANCE_ID, project_id=TEST_PROJECT_ID
        )
