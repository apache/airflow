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

"""Tests for Bigtable links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.bigtable import (
    BIGTABLE_CLUSTER_LINK,
    BIGTABLE_INSTANCE_LINK,
    BIGTABLE_TABLES_LINK,
    BigtableClusterLink,
    BigtableInstanceLink,
    BigtableTablesLink,
)

TEST_CLUSTER_ID = "test-cluster-id"
TEST_INSTANCE_ID = "test-instance-id"
TEST_PROJECT_ID = "test-project-id"


class TestBigtableInstanceLink:
    def test_class_attributes(self):
        assert BigtableInstanceLink.key == "instance_key"
        assert BigtableInstanceLink.name == "Bigtable Instance"
        assert BigtableInstanceLink.format_str == BIGTABLE_INSTANCE_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        BigtableInstanceLink.persist(
            context=mock_context,
            instance_id=TEST_INSTANCE_ID,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="instance_key",
            value={"instance_id": TEST_INSTANCE_ID, "project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = BigtableInstanceLink()

        result = link._format_link(instance_id=TEST_INSTANCE_ID, project_id=TEST_PROJECT_ID)

        assert result == BASE_LINK + BIGTABLE_INSTANCE_LINK.format(
            instance_id=TEST_INSTANCE_ID, project_id=TEST_PROJECT_ID
        )


class TestBigtableClusterLink:
    def test_class_attributes(self):
        assert BigtableClusterLink.key == "cluster_key"
        assert BigtableClusterLink.name == "Bigtable Cluster"
        assert BigtableClusterLink.format_str == BIGTABLE_CLUSTER_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        BigtableClusterLink.persist(
            context=mock_context,
            cluster_id=TEST_CLUSTER_ID,
            instance_id=TEST_INSTANCE_ID,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cluster_key",
            value={
                "cluster_id": TEST_CLUSTER_ID,
                "instance_id": TEST_INSTANCE_ID,
                "project_id": TEST_PROJECT_ID,
            },
        )

    def test_format_link(self):
        link = BigtableClusterLink()

        result = link._format_link(
            cluster_id=TEST_CLUSTER_ID, instance_id=TEST_INSTANCE_ID, project_id=TEST_PROJECT_ID
        )

        assert result == BASE_LINK + BIGTABLE_CLUSTER_LINK.format(
            cluster_id=TEST_CLUSTER_ID, instance_id=TEST_INSTANCE_ID, project_id=TEST_PROJECT_ID
        )


class TestBigtableTablesLink:
    def test_class_attributes(self):
        assert BigtableTablesLink.key == "tables_key"
        assert BigtableTablesLink.name == "Bigtable Tables"
        assert BigtableTablesLink.format_str == BIGTABLE_TABLES_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        BigtableTablesLink.persist(
            context=mock_context,
            instance_id=TEST_INSTANCE_ID,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="tables_key",
            value={"instance_id": TEST_INSTANCE_ID, "project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = BigtableTablesLink()

        result = link._format_link(instance_id=TEST_INSTANCE_ID, project_id=TEST_PROJECT_ID)

        assert result == BASE_LINK + BIGTABLE_TABLES_LINK.format(
            instance_id=TEST_INSTANCE_ID, project_id=TEST_PROJECT_ID
        )
