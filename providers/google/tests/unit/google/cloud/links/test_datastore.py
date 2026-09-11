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
"""Tests for Cloud Datastore links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.datastore import (
    DATASTORE_ENTITIES_LINK,
    DATASTORE_IMPORT_EXPORT_LINK,
    CloudDatastoreEntitiesLink,
    CloudDatastoreImportExportLink,
)

TEST_PROJECT_ID = "test-project"


class TestCloudDatastoreImportExportLink:
    def test_class_attributes(self):
        assert CloudDatastoreImportExportLink.key == "import_export_conf"
        assert CloudDatastoreImportExportLink.name == "Import/Export Page"
        assert CloudDatastoreImportExportLink.format_str == DATASTORE_IMPORT_EXPORT_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudDatastoreImportExportLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="import_export_conf",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = CloudDatastoreImportExportLink()
        result = link._format_link(project_id=TEST_PROJECT_ID)
        assert result == BASE_LINK + DATASTORE_IMPORT_EXPORT_LINK.format(project_id=TEST_PROJECT_ID)


class TestCloudDatastoreEntitiesLink:
    def test_class_attributes(self):
        assert CloudDatastoreEntitiesLink.key == "entities_conf"
        assert CloudDatastoreEntitiesLink.name == "Entities"
        assert CloudDatastoreEntitiesLink.format_str == DATASTORE_ENTITIES_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudDatastoreEntitiesLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="entities_conf",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = CloudDatastoreEntitiesLink()
        result = link._format_link(project_id=TEST_PROJECT_ID)
        assert result == BASE_LINK + DATASTORE_ENTITIES_LINK.format(project_id=TEST_PROJECT_ID)
