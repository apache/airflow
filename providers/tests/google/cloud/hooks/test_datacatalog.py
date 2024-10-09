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

import re
from copy import deepcopy
from typing import TYPE_CHECKING, Sequence
from unittest import mock

import pytest
from google.api_core.retry import Retry
from google.cloud.datacatalog import CreateTagRequest, CreateTagTemplateRequest, Entry, Tag, TagTemplate
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.datacatalog import CloudDataCatalogHook

from providers.tests.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

if TYPE_CHECKING:
    from google.api_core.gapic_v1.method import _MethodDefault

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_LOCATION: str = "europe-west-3b"
TEST_ENTRY_ID: str = "test-entry-id"
TEST_ENTRY: dict = {}
TEST_RETRY: Retry | _MethodDefault = Retry()
TEST_TIMEOUT: float = 4
TEST_METADATA: Sequence[tuple[str, str]] = ()
TEST_ENTRY_GROUP_ID: str = "test-entry-group-id"
TEST_ENTRY_GROUP: dict = {}
TEST_TAG: dict = {}
TEST_TAG_TEMPLATE_ID: str = "test-tag-template-id"
TEST_TAG_TEMPLATE: dict = {"name": TEST_TAG_TEMPLATE_ID}
TEST_TAG_TEMPLATE_FIELD_ID: str = "test-tag-template-field-id"
TEST_TAG_TEMPLATE_FIELD: dict = {}
TEST_FORCE: bool = False
TEST_READ_MASK: FieldMask = FieldMask(paths=["name"])
TEST_RESOURCE: str = "test-resource"
TEST_PAGE_SIZE: int = 50
TEST_LINKED_RESOURCE: str = "test-linked-resource"
TEST_SQL_RESOURCE: str = "test-sql-resource"
TEST_NEW_TAG_TEMPLATE_FIELD_ID: str = "test-new-tag-template-field-id"
TEST_SCOPE: dict = {"include_project_ids": ["example-scope-project"]}
TEST_QUERY: str = "test-query"
TEST_ORDER_BY: str = "test-order-by"
TEST_UPDATE_MASK: dict = {"fields": ["name"]}
TEST_PARENT: str = "test-parent"
TEST_NAME: str = "test-name"
TEST_TAG_ID: str = "test-tag-id"
TEST_LOCATION_PATH: str = f"projects/{{}}/locations/{TEST_LOCATION}"
TEST_ENTRY_PATH: str = (
    f"projects/{{}}/locations/{TEST_LOCATION}/entryGroups/{TEST_ENTRY_GROUP_ID}/entries/{TEST_ENTRY_ID}"
)
TEST_ENTRY_GROUP_PATH: str = f"projects/{{}}/locations/{TEST_LOCATION}/entryGroups/{TEST_ENTRY_GROUP_ID}"
TEST_TAG_TEMPLATE_PATH: str = f"projects/{{}}/locations/{TEST_LOCATION}/tagTemplates/{TEST_TAG_TEMPLATE_ID}"
TEST_TAG_TEMPLATE_FIELD_PATH: str = (
    f"projects/{{}}/locations/{TEST_LOCATION}/tagTemplates/"
    f"{TEST_TAG_TEMPLATE_ID}/fields/{TEST_TAG_TEMPLATE_FIELD_ID}"
)
TEST_TAG_PATH: str = (
    f"projects/{{}}/locations/{TEST_LOCATION}/entryGroups/{TEST_ENTRY_GROUP_ID}"
    f"/entries/{TEST_ENTRY_ID}/tags/{TEST_TAG_ID}"
)
TEST_PROJECT_ID_1 = "example-project-1"
TEST_PROJECT_ID_2 = "example-project-2"
TEST_CREDENTIALS = mock.MagicMock()


class TestCloudDataCatalog:
    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            CloudDataCatalogHook(gcp_conn_id="test", delegate_to="delegate_to")

    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudDataCatalogHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_lookup_entry_with_linked_resource(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.lookup_entry(
            linked_resource=TEST_LINKED_RESOURCE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.lookup_entry.assert_called_once_with(
            request=dict(linked_resource=TEST_LINKED_RESOURCE),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_lookup_entry_with_sql_resource(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.lookup_entry(
            sql_resource=TEST_SQL_RESOURCE, retry=TEST_RETRY, timeout=TEST_TIMEOUT, metadata=TEST_METADATA
        )
        mock_get_conn.return_value.lookup_entry.assert_called_once_with(
            request=dict(sql_resource=TEST_SQL_RESOURCE),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_lookup_entry_without_resource(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(
            AirflowException, match=re.escape("At least one of linked_resource, sql_resource should be set.")
        ):
            self.hook.lookup_entry(retry=TEST_RETRY, timeout=TEST_TIMEOUT, metadata=TEST_METADATA)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_search_catalog(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.search_catalog(
            scope=TEST_SCOPE,
            query=TEST_QUERY,
            page_size=TEST_PAGE_SIZE,
            order_by=TEST_ORDER_BY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.search_catalog.assert_called_once_with(
            request=dict(
                scope=TEST_SCOPE, query=TEST_QUERY, page_size=TEST_PAGE_SIZE, order_by=TEST_ORDER_BY
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogWithDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudDataCatalogHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_entry(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.create_entry(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry_id=TEST_ENTRY_ID,
            entry=TEST_ENTRY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.create_entry.assert_called_once_with(
            request=dict(
                parent=TEST_ENTRY_GROUP_PATH.format(TEST_PROJECT_ID_1),
                entry_id=TEST_ENTRY_ID,
                entry=TEST_ENTRY,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_entry_group(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.create_entry_group(
            location=TEST_LOCATION,
            entry_group_id=TEST_ENTRY_GROUP_ID,
            entry_group=TEST_ENTRY_GROUP,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.create_entry_group.assert_called_once_with(
            request=dict(
                parent=TEST_LOCATION_PATH.format(TEST_PROJECT_ID_1),
                entry_group_id=TEST_ENTRY_GROUP_ID,
                entry_group=TEST_ENTRY_GROUP,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_tag(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.create_tag(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            tag=deepcopy(TEST_TAG),
            template_id=TEST_TAG_TEMPLATE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.create_tag.assert_called_once_with(
            request=CreateTagRequest(
                parent=TEST_ENTRY_PATH.format(TEST_PROJECT_ID_1),
                tag=Tag(template=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_1)),
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_tag_protobuff(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.create_tag(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            tag=Tag(),
            template_id=TEST_TAG_TEMPLATE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.create_tag.assert_called_once_with(
            request=CreateTagRequest(
                parent=TEST_ENTRY_PATH.format(TEST_PROJECT_ID_1),
                tag=Tag(template=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_1)),
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_tag_template(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.create_tag_template(
            location=TEST_LOCATION,
            tag_template_id=TEST_TAG_TEMPLATE_ID,
            tag_template=TEST_TAG_TEMPLATE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.create_tag_template.assert_called_once_with(
            request=CreateTagTemplateRequest(
                parent=TEST_LOCATION_PATH.format(TEST_PROJECT_ID_1),
                tag_template_id=TEST_TAG_TEMPLATE_ID,
                tag_template=TEST_TAG_TEMPLATE,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_tag_template_field(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.create_tag_template_field(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            tag_template_field_id=TEST_TAG_TEMPLATE_FIELD_ID,
            tag_template_field=TEST_TAG_TEMPLATE_FIELD,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.create_tag_template_field.assert_called_once_with(
            request=dict(
                parent=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_1),
                tag_template_field_id=TEST_TAG_TEMPLATE_FIELD_ID,
                tag_template_field=TEST_TAG_TEMPLATE_FIELD,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_entry(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.delete_entry(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.delete_entry.assert_called_once_with(
            request=dict(
                name=TEST_ENTRY_PATH.format(TEST_PROJECT_ID_1),
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_entry_group(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.delete_entry_group(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.delete_entry_group.assert_called_once_with(
            request=dict(
                name=TEST_ENTRY_GROUP_PATH.format(TEST_PROJECT_ID_1),
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_tag(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.delete_tag(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            tag=TEST_TAG_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.delete_tag.assert_called_once_with(
            request=dict(
                name=TEST_TAG_PATH.format(TEST_PROJECT_ID_1),
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_tag_template(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.delete_tag_template(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            force=TEST_FORCE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.delete_tag_template.assert_called_once_with(
            request=dict(name=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_1), force=TEST_FORCE),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_tag_template_field(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.delete_tag_template_field(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            field=TEST_TAG_TEMPLATE_FIELD_ID,
            force=TEST_FORCE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.delete_tag_template_field.assert_called_once_with(
            request=dict(
                name=TEST_TAG_TEMPLATE_FIELD_PATH.format(TEST_PROJECT_ID_1),
                force=TEST_FORCE,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_get_entry(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.get_entry(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.get_entry.assert_called_once_with(
            request=dict(
                name=TEST_ENTRY_PATH.format(TEST_PROJECT_ID_1),
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_get_entry_group(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.get_entry_group(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            read_mask=TEST_READ_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.get_entry_group.assert_called_once_with(
            request=dict(
                name=TEST_ENTRY_GROUP_PATH.format(TEST_PROJECT_ID_1),
                read_mask=TEST_READ_MASK,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_get_tag_template(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.get_tag_template(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.get_tag_template.assert_called_once_with(
            request=dict(
                name=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_1),
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_list_tags(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.list_tags(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            page_size=TEST_PAGE_SIZE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.list_tags.assert_called_once_with(
            request=dict(
                parent=TEST_ENTRY_PATH.format(TEST_PROJECT_ID_1),
                page_size=TEST_PAGE_SIZE,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_get_tag_for_template_name(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        tag_1 = mock.MagicMock(template=TEST_TAG_TEMPLATE_PATH.format("invalid-project"))
        tag_2 = mock.MagicMock(template=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_1))

        mock_get_conn.return_value.list_tags.return_value = [tag_1, tag_2]
        result = self.hook.get_tag_for_template_name(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            template_name=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_1),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.list_tags.assert_called_once_with(
            request=dict(
                parent=TEST_ENTRY_PATH.format(TEST_PROJECT_ID_1),
                page_size=100,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        assert result == tag_2

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_rename_tag_template_field(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.rename_tag_template_field(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            field=TEST_TAG_TEMPLATE_FIELD_ID,
            new_tag_template_field_id=TEST_NEW_TAG_TEMPLATE_FIELD_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.rename_tag_template_field.assert_called_once_with(
            request=dict(
                name=TEST_TAG_TEMPLATE_FIELD_PATH.format(TEST_PROJECT_ID_1),
                new_tag_template_field_id=TEST_NEW_TAG_TEMPLATE_FIELD_ID,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_update_entry(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.update_entry(
            entry=TEST_ENTRY,
            update_mask=TEST_UPDATE_MASK,
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry_id=TEST_ENTRY_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.update_entry.assert_called_once_with(
            request=dict(
                entry=Entry(name=TEST_ENTRY_PATH.format(TEST_PROJECT_ID_1)),
                update_mask=TEST_UPDATE_MASK,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_update_tag(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.update_tag(
            tag=deepcopy(TEST_TAG),
            update_mask=TEST_UPDATE_MASK,
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            tag_id=TEST_TAG_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.update_tag.assert_called_once_with(
            request=dict(tag=Tag(name=TEST_TAG_PATH.format(TEST_PROJECT_ID_1)), update_mask=TEST_UPDATE_MASK),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_update_tag_template(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.update_tag_template(
            tag_template=TEST_TAG_TEMPLATE,
            update_mask=TEST_UPDATE_MASK,
            location=TEST_LOCATION,
            tag_template_id=TEST_TAG_TEMPLATE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.update_tag_template.assert_called_once_with(
            request=dict(
                tag_template=TagTemplate(name=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_1)),
                update_mask=TEST_UPDATE_MASK,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_1),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_update_tag_template_field(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.update_tag_template_field(
            tag_template_field=TEST_TAG_TEMPLATE_FIELD,
            update_mask=TEST_UPDATE_MASK,
            tag_template=TEST_TAG_TEMPLATE_ID,
            location=TEST_LOCATION,
            tag_template_field_id=TEST_TAG_TEMPLATE_FIELD_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.update_tag_template_field.assert_called_once_with(
            request=dict(
                name=TEST_TAG_TEMPLATE_FIELD_PATH.format(TEST_PROJECT_ID_1),
                tag_template_field=TEST_TAG_TEMPLATE_FIELD,
                update_mask=TEST_UPDATE_MASK,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogWithoutDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = CloudDataCatalogHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_entry(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.create_entry(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry_id=TEST_ENTRY_ID,
            entry=TEST_ENTRY,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.create_entry.assert_called_once_with(
            request=dict(
                parent=TEST_ENTRY_GROUP_PATH.format(TEST_PROJECT_ID_2),
                entry_id=TEST_ENTRY_ID,
                entry=TEST_ENTRY,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_entry_group(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.create_entry_group(
            location=TEST_LOCATION,
            entry_group_id=TEST_ENTRY_GROUP_ID,
            entry_group=TEST_ENTRY_GROUP,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.create_entry_group.assert_called_once_with(
            request=dict(
                parent=TEST_LOCATION_PATH.format(TEST_PROJECT_ID_2),
                entry_group_id=TEST_ENTRY_GROUP_ID,
                entry_group=TEST_ENTRY_GROUP,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_tag(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.create_tag(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            tag=deepcopy(TEST_TAG),
            template_id=TEST_TAG_TEMPLATE_ID,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.create_tag.assert_called_once_with(
            request=CreateTagRequest(
                parent=TEST_ENTRY_PATH.format(TEST_PROJECT_ID_2),
                tag=Tag(template=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_2)),
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_tag_protobuff(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.create_tag(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            tag=Tag(),
            template_id=TEST_TAG_TEMPLATE_ID,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.create_tag.assert_called_once_with(
            request=CreateTagRequest(
                parent=TEST_ENTRY_PATH.format(TEST_PROJECT_ID_2),
                tag=Tag(template=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_2)),
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_tag_template(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.create_tag_template(
            location=TEST_LOCATION,
            tag_template_id=TEST_TAG_TEMPLATE_ID,
            tag_template=TEST_TAG_TEMPLATE,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.create_tag_template.assert_called_once_with(
            request=CreateTagTemplateRequest(
                parent=TEST_LOCATION_PATH.format(TEST_PROJECT_ID_2),
                tag_template_id=TEST_TAG_TEMPLATE_ID,
                tag_template=TEST_TAG_TEMPLATE,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_tag_template_field(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.create_tag_template_field(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            tag_template_field_id=TEST_TAG_TEMPLATE_FIELD_ID,
            tag_template_field=TEST_TAG_TEMPLATE_FIELD,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.create_tag_template_field.assert_called_once_with(
            request=dict(
                parent=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_2),
                tag_template_field_id=TEST_TAG_TEMPLATE_FIELD_ID,
                tag_template_field=TEST_TAG_TEMPLATE_FIELD,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_entry(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.delete_entry(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.delete_entry.assert_called_once_with(
            request=dict(name=TEST_ENTRY_PATH.format(TEST_PROJECT_ID_2)),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_entry_group(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.delete_entry_group(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.delete_entry_group.assert_called_once_with(
            request=dict(name=TEST_ENTRY_GROUP_PATH.format(TEST_PROJECT_ID_2)),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_tag(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.delete_tag(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            tag=TEST_TAG_ID,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.delete_tag.assert_called_once_with(
            request=dict(name=TEST_TAG_PATH.format(TEST_PROJECT_ID_2)),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_tag_template(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.delete_tag_template(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            force=TEST_FORCE,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.delete_tag_template.assert_called_once_with(
            request=dict(name=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_2), force=TEST_FORCE),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_tag_template_field(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.delete_tag_template_field(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            field=TEST_TAG_TEMPLATE_FIELD_ID,
            force=TEST_FORCE,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.delete_tag_template_field.assert_called_once_with(
            request=dict(name=TEST_TAG_TEMPLATE_FIELD_PATH.format(TEST_PROJECT_ID_2), force=TEST_FORCE),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_get_entry(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.get_entry(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.get_entry.assert_called_once_with(
            request=dict(name=TEST_ENTRY_PATH.format(TEST_PROJECT_ID_2)),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_get_entry_group(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.get_entry_group(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            read_mask=TEST_READ_MASK,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.get_entry_group.assert_called_once_with(
            request=dict(
                name=TEST_ENTRY_GROUP_PATH.format(TEST_PROJECT_ID_2),
                read_mask=TEST_READ_MASK,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_get_tag_template(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.get_tag_template(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.get_tag_template.assert_called_once_with(
            request=dict(name=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_2)),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_list_tags(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.list_tags(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            page_size=TEST_PAGE_SIZE,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.list_tags.assert_called_once_with(
            request=dict(parent=TEST_ENTRY_PATH.format(TEST_PROJECT_ID_2), page_size=TEST_PAGE_SIZE),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_get_tag_for_template_name(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        tag_1 = mock.MagicMock(template=TEST_TAG_TEMPLATE_PATH.format("invalid-project"))
        tag_2 = mock.MagicMock(template=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_2))

        mock_get_conn.return_value.list_tags.return_value = [tag_1, tag_2]
        result = self.hook.get_tag_for_template_name(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            template_name=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_2),
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.list_tags.assert_called_once_with(
            request=dict(parent=TEST_ENTRY_PATH.format(TEST_PROJECT_ID_2), page_size=100),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        assert result == tag_2

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_rename_tag_template_field(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.rename_tag_template_field(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            field=TEST_TAG_TEMPLATE_FIELD_ID,
            new_tag_template_field_id=TEST_NEW_TAG_TEMPLATE_FIELD_ID,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.rename_tag_template_field.assert_called_once_with(
            request=dict(
                name=TEST_TAG_TEMPLATE_FIELD_PATH.format(TEST_PROJECT_ID_2),
                new_tag_template_field_id=TEST_NEW_TAG_TEMPLATE_FIELD_ID,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_update_entry(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.update_entry(
            entry=TEST_ENTRY,
            update_mask=TEST_UPDATE_MASK,
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry_id=TEST_ENTRY_ID,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.update_entry.assert_called_once_with(
            request=dict(
                entry=Entry(name=TEST_ENTRY_PATH.format(TEST_PROJECT_ID_2)), update_mask=TEST_UPDATE_MASK
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_update_tag(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.update_tag(
            tag=deepcopy(TEST_TAG),
            update_mask=TEST_UPDATE_MASK,
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            tag_id=TEST_TAG_ID,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.update_tag.assert_called_once_with(
            request=dict(tag=Tag(name=TEST_TAG_PATH.format(TEST_PROJECT_ID_2)), update_mask=TEST_UPDATE_MASK),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_update_tag_template(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.update_tag_template(
            tag_template=TEST_TAG_TEMPLATE,
            update_mask=TEST_UPDATE_MASK,
            location=TEST_LOCATION,
            tag_template_id=TEST_TAG_TEMPLATE_ID,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.update_tag_template.assert_called_once_with(
            request=dict(
                tag_template=TagTemplate(name=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_2)),
                update_mask=TEST_UPDATE_MASK,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_update_tag_template_field(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.update_tag_template_field(
            tag_template_field=TEST_TAG_TEMPLATE_FIELD,
            update_mask=TEST_UPDATE_MASK,
            tag_template=TEST_TAG_TEMPLATE_ID,
            location=TEST_LOCATION,
            tag_template_field_id=TEST_TAG_TEMPLATE_FIELD_ID,
            project_id=TEST_PROJECT_ID_2,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.update_tag_template_field.assert_called_once_with(
            request=dict(
                name=TEST_TAG_TEMPLATE_FIELD_PATH.format(TEST_PROJECT_ID_2),
                tag_template_field=TEST_TAG_TEMPLATE_FIELD,
                update_mask=TEST_UPDATE_MASK,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


TEST_MESSAGE = re.escape(
    "The project id must be passed either as keyword project_id parameter or as project_id extra in "
    "Google Cloud connection definition. Both are not set!"
)


class TestCloudDataCatalogMissingProjectIdHook:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = CloudDataCatalogHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_entry(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.create_entry(
                location=TEST_LOCATION,
                entry_group=TEST_ENTRY_GROUP_ID,
                entry_id=TEST_ENTRY_ID,
                entry=TEST_ENTRY,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_entry_group(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.create_entry_group(
                location=TEST_LOCATION,
                entry_group_id=TEST_ENTRY_GROUP_ID,
                entry_group=TEST_ENTRY_GROUP,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_tag(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.create_tag(
                location=TEST_LOCATION,
                entry_group=TEST_ENTRY_GROUP_ID,
                entry=TEST_ENTRY_ID,
                tag=deepcopy(TEST_TAG),
                template_id=TEST_TAG_TEMPLATE_ID,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_tag_protobuff(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.create_tag(
                location=TEST_LOCATION,
                entry_group=TEST_ENTRY_GROUP_ID,
                entry=TEST_ENTRY_ID,
                tag=Tag(),
                template_id=TEST_TAG_TEMPLATE_ID,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_tag_template(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.create_tag_template(
                location=TEST_LOCATION,
                tag_template_id=TEST_TAG_TEMPLATE_ID,
                tag_template=TEST_TAG_TEMPLATE,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_create_tag_template_field(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.create_tag_template_field(
                location=TEST_LOCATION,
                tag_template=TEST_TAG_TEMPLATE_ID,
                tag_template_field_id=TEST_TAG_TEMPLATE_FIELD_ID,
                tag_template_field=TEST_TAG_TEMPLATE_FIELD,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_entry(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.delete_entry(
                location=TEST_LOCATION,
                entry_group=TEST_ENTRY_GROUP_ID,
                entry=TEST_ENTRY_ID,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_entry_group(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.delete_entry_group(
                location=TEST_LOCATION,
                entry_group=TEST_ENTRY_GROUP_ID,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_tag(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.delete_tag(
                location=TEST_LOCATION,
                entry_group=TEST_ENTRY_GROUP_ID,
                entry=TEST_ENTRY_ID,
                tag=TEST_TAG_ID,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_tag_template(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.delete_tag_template(
                location=TEST_LOCATION,
                tag_template=TEST_TAG_TEMPLATE_ID,
                force=TEST_FORCE,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_delete_tag_template_field(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.delete_tag_template_field(
                location=TEST_LOCATION,
                tag_template=TEST_TAG_TEMPLATE_ID,
                field=TEST_TAG_TEMPLATE_FIELD_ID,
                force=TEST_FORCE,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_get_entry(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.get_entry(
                location=TEST_LOCATION,
                entry_group=TEST_ENTRY_GROUP_ID,
                entry=TEST_ENTRY_ID,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_get_entry_group(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.get_entry_group(
                location=TEST_LOCATION,
                entry_group=TEST_ENTRY_GROUP_ID,
                read_mask=TEST_READ_MASK,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_get_tag_template(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.get_tag_template(
                location=TEST_LOCATION,
                tag_template=TEST_TAG_TEMPLATE_ID,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_list_tags(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.list_tags(
                location=TEST_LOCATION,
                entry_group=TEST_ENTRY_GROUP_ID,
                entry=TEST_ENTRY_ID,
                page_size=TEST_PAGE_SIZE,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_get_tag_for_template_name(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        tag_1 = mock.MagicMock(template=TEST_TAG_TEMPLATE_PATH.format("invalid-project"))
        tag_2 = mock.MagicMock(template=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_2))

        mock_get_conn.return_value.list_tags.return_value = [tag_1, tag_2]
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.get_tag_for_template_name(
                location=TEST_LOCATION,
                entry_group=TEST_ENTRY_GROUP_ID,
                entry=TEST_ENTRY_ID,
                template_name=TEST_TAG_TEMPLATE_PATH.format(TEST_PROJECT_ID_2),
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_rename_tag_template_field(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.rename_tag_template_field(
                location=TEST_LOCATION,
                tag_template=TEST_TAG_TEMPLATE_ID,
                field=TEST_TAG_TEMPLATE_FIELD_ID,
                new_tag_template_field_id=TEST_NEW_TAG_TEMPLATE_FIELD_ID,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_update_entry(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.update_entry(
                entry=TEST_ENTRY,
                update_mask=TEST_UPDATE_MASK,
                location=TEST_LOCATION,
                entry_group=TEST_ENTRY_GROUP_ID,
                entry_id=TEST_ENTRY_ID,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_update_tag(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.update_tag(
                tag=deepcopy(TEST_TAG),
                update_mask=TEST_UPDATE_MASK,
                location=TEST_LOCATION,
                entry_group=TEST_ENTRY_GROUP_ID,
                entry=TEST_ENTRY_ID,
                tag_id=TEST_TAG_ID,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_update_tag_template(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.update_tag_template(
                tag_template=TEST_TAG_TEMPLATE,
                update_mask=TEST_UPDATE_MASK,
                location=TEST_LOCATION,
                tag_template_id=TEST_TAG_TEMPLATE_ID,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook.get_conn")
    def test_update_tag_template_field(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.update_tag_template_field(
                tag_template_field=TEST_TAG_TEMPLATE_FIELD,
                update_mask=TEST_UPDATE_MASK,
                tag_template=TEST_TAG_TEMPLATE_ID,
                location=TEST_LOCATION,
                tag_template_field_id=TEST_TAG_TEMPLATE_FIELD_ID,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )
