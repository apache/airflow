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

from typing import Dict, Sequence, Tuple
from unittest import TestCase, mock

from google.api_core.exceptions import AlreadyExists
from google.api_core.retry import Retry
from google.cloud.datacatalog_v1beta1.types import Entry, EntryGroup, Tag, TagTemplate, TagTemplateField

from airflow.providers.google.cloud.operators.datacatalog import (
    CloudDataCatalogCreateEntryGroupOperator,
    CloudDataCatalogCreateEntryOperator,
    CloudDataCatalogCreateTagOperator,
    CloudDataCatalogCreateTagTemplateFieldOperator,
    CloudDataCatalogCreateTagTemplateOperator,
    CloudDataCatalogDeleteEntryGroupOperator,
    CloudDataCatalogDeleteEntryOperator,
    CloudDataCatalogDeleteTagOperator,
    CloudDataCatalogDeleteTagTemplateFieldOperator,
    CloudDataCatalogDeleteTagTemplateOperator,
    CloudDataCatalogGetEntryGroupOperator,
    CloudDataCatalogGetEntryOperator,
    CloudDataCatalogGetTagTemplateOperator,
    CloudDataCatalogListTagsOperator,
    CloudDataCatalogLookupEntryOperator,
    CloudDataCatalogRenameTagTemplateFieldOperator,
    CloudDataCatalogSearchCatalogOperator,
    CloudDataCatalogUpdateEntryOperator,
    CloudDataCatalogUpdateTagOperator,
    CloudDataCatalogUpdateTagTemplateFieldOperator,
    CloudDataCatalogUpdateTagTemplateOperator,
)

TEST_PROJECT_ID: str = "example_id"
TEST_LOCATION: str = "en-west-3"
TEST_ENTRY_ID: str = "test-entry-id"
TEST_TAG_ID: str = "test-tag-id"
TEST_RETRY: Retry = Retry()
TEST_TIMEOUT: float = 0.5
TEST_METADATA: Sequence[Tuple[str, str]] = []
TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_IMPERSONATION_CHAIN: Sequence[str] = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
TEST_ENTRY_GROUP_ID: str = "test-entry-group-id"
TEST_TAG_TEMPLATE_ID: str = "test-tag-template-id"
TEST_TAG_TEMPLATE_FIELD_ID: str = "test-tag-template-field-id"
TEST_TAG_TEMPLATE_NAME: str = "test-tag-template-field-name"
TEST_FORCE: bool = False
TEST_READ_MASK: Dict = {"fields": ["name"]}
TEST_RESOURCE: str = "test-resource"
TEST_OPTIONS_: Dict = {}
TEST_PAGE_SIZE: int = 50
TEST_LINKED_RESOURCE: str = "test-linked-resource"
TEST_SQL_RESOURCE: str = "test-sql-resource"
TEST_NEW_TAG_TEMPLATE_FIELD_ID: str = "test-new-tag-template-field-id"
TEST_SCOPE: Dict = dict(include_project_ids=["example-scope-project"])
TEST_QUERY: str = "test-query"
TEST_ORDER_BY: str = "test-order-by"
TEST_UPDATE_MASK: Dict = {"fields": ["name"]}
TEST_ENTRY_PATH: str = (
    f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}"
    + f"/entryGroups/{TEST_ENTRY_GROUP_ID}/entries/{TEST_ENTRY_ID}"
)
TEST_ENTRY_GROUP_PATH: str = (
    f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/entryGroups/{TEST_ENTRY_GROUP_ID}"
)
TEST_TAG_TEMPLATE_PATH: str = (
    f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/tagTemplates/{TEST_TAG_TEMPLATE_ID}"
)
TEST_TAG_PATH: str = (
    f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/entryGroups/"
    + f"{TEST_ENTRY_GROUP_ID}/entries/{TEST_ENTRY_ID}/tags/{TEST_TAG_ID}"
)

TEST_ENTRY: Entry = Entry(name=TEST_ENTRY_PATH)
TEST_ENTRY_DICT: Dict = {
    'description': '',
    'display_name': '',
    'linked_resource': '',
    'name': TEST_ENTRY_PATH,
}
TEST_ENTRY_GROUP: EntryGroup = EntryGroup(name=TEST_ENTRY_GROUP_PATH)
TEST_ENTRY_GROUP_DICT: Dict = {'description': '', 'display_name': '', 'name': TEST_ENTRY_GROUP_PATH}
TEST_TAG: Tag = Tag(name=TEST_TAG_PATH)
TEST_TAG_DICT: Dict = {'fields': {}, 'name': TEST_TAG_PATH, 'template': '', 'template_display_name': ''}
TEST_TAG_TEMPLATE: TagTemplate = TagTemplate(name=TEST_TAG_TEMPLATE_PATH)
TEST_TAG_TEMPLATE_DICT: Dict = {'display_name': '', 'fields': {}, 'name': TEST_TAG_TEMPLATE_PATH}
TEST_TAG_TEMPLATE_FIELD: TagTemplateField = TagTemplateField(name=TEST_TAG_TEMPLATE_FIELD_ID)
TEST_TAG_TEMPLATE_FIELD_DICT: Dict = {
    'display_name': '',
    'is_required': False,
    'name': TEST_TAG_TEMPLATE_FIELD_ID,
    'order': 0,
}


class TestCloudDataCatalogCreateEntryOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook",
        **{"return_value.create_entry.return_value": TEST_ENTRY},
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogCreateEntryOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry_id=TEST_ENTRY_ID,
            entry=TEST_ENTRY,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        ti = mock.MagicMock()
        result = task.execute(context={"task_instance": ti})
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_entry.assert_called_once_with(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry_id=TEST_ENTRY_ID,
            entry=TEST_ENTRY,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        ti.xcom_push.assert_called_once_with(key="entry_id", value=TEST_ENTRY_ID)
        assert TEST_ENTRY_DICT == result

    @mock.patch(
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook",
        **{
            "return_value.create_entry.side_effect": AlreadyExists(message="message"),
            "return_value.get_entry.return_value": TEST_ENTRY,
        },
    )
    def test_assert_valid_hook_call_when_exists(self, mock_hook) -> None:
        task = CloudDataCatalogCreateEntryOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry_id=TEST_ENTRY_ID,
            entry=TEST_ENTRY,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        ti = mock.MagicMock()
        result = task.execute(context={"task_instance": ti})
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_entry.assert_called_once_with(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry_id=TEST_ENTRY_ID,
            entry=TEST_ENTRY,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_hook.return_value.get_entry.assert_called_once_with(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        ti.xcom_push.assert_called_once_with(key="entry_id", value=TEST_ENTRY_ID)
        assert TEST_ENTRY_DICT == result


class TestCloudDataCatalogCreateEntryGroupOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook",
        **{"return_value.create_entry_group.return_value": TEST_ENTRY_GROUP},
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogCreateEntryGroupOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            entry_group_id=TEST_ENTRY_GROUP_ID,
            entry_group=TEST_ENTRY_GROUP,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        ti = mock.MagicMock()
        result = task.execute(context={"task_instance": ti})
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_entry_group.assert_called_once_with(
            location=TEST_LOCATION,
            entry_group_id=TEST_ENTRY_GROUP_ID,
            entry_group=TEST_ENTRY_GROUP,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        ti.xcom_push.assert_called_once_with(key="entry_group_id", value=TEST_ENTRY_GROUP_ID)
        assert result == TEST_ENTRY_GROUP_DICT


class TestCloudDataCatalogCreateTagOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook",
        **{"return_value.create_tag.return_value": TEST_TAG},
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogCreateTagOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            tag=TEST_TAG,
            template_id=TEST_TAG_TEMPLATE_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        ti = mock.MagicMock()
        result = task.execute(context={"task_instance": ti})
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_tag.assert_called_once_with(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            tag=TEST_TAG,
            template_id=TEST_TAG_TEMPLATE_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        ti.xcom_push.assert_called_once_with(key="tag_id", value=TEST_TAG_ID)
        assert TEST_TAG_DICT == result


class TestCloudDataCatalogCreateTagTemplateOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook",
        **{"return_value.create_tag_template.return_value": TEST_TAG_TEMPLATE},
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogCreateTagTemplateOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            tag_template_id=TEST_TAG_TEMPLATE_ID,
            tag_template=TEST_TAG_TEMPLATE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        ti = mock.MagicMock()
        result = task.execute(context={"task_instance": ti})
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_tag_template.assert_called_once_with(
            location=TEST_LOCATION,
            tag_template_id=TEST_TAG_TEMPLATE_ID,
            tag_template=TEST_TAG_TEMPLATE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        ti.xcom_push.assert_called_once_with(key="tag_template_id", value=TEST_TAG_TEMPLATE_ID)
        assert TEST_TAG_TEMPLATE_DICT == result


class TestCloudDataCatalogCreateTagTemplateFieldOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook",
        **{"return_value.create_tag_template_field.return_value": TEST_TAG_TEMPLATE_FIELD},  # type: ignore
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogCreateTagTemplateFieldOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            tag_template_field_id=TEST_TAG_TEMPLATE_FIELD_ID,
            tag_template_field=TEST_TAG_TEMPLATE_FIELD,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        ti = mock.MagicMock()
        result = task.execute(context={"task_instance": ti})
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_tag_template_field.assert_called_once_with(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            tag_template_field_id=TEST_TAG_TEMPLATE_FIELD_ID,
            tag_template_field=TEST_TAG_TEMPLATE_FIELD,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        ti.xcom_push.assert_called_once_with(key="tag_template_field_id", value=TEST_TAG_TEMPLATE_FIELD_ID)
        assert TEST_TAG_TEMPLATE_FIELD_DICT == result


class TestCloudDataCatalogDeleteEntryOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook")
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogDeleteEntryOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_entry.assert_called_once_with(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogDeleteEntryGroupOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook")
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogDeleteEntryGroupOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_entry_group.assert_called_once_with(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogDeleteTagOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook")
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogDeleteTagOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            tag=TEST_TAG_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_tag.assert_called_once_with(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            tag=TEST_TAG_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogDeleteTagTemplateOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook")
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogDeleteTagTemplateOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            force=TEST_FORCE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_tag_template.assert_called_once_with(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            force=TEST_FORCE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogDeleteTagTemplateFieldOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook")
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogDeleteTagTemplateFieldOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            field=TEST_TAG_TEMPLATE_FIELD_ID,
            force=TEST_FORCE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_tag_template_field.assert_called_once_with(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            field=TEST_TAG_TEMPLATE_FIELD_ID,
            force=TEST_FORCE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogGetEntryOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook",
        **{"return_value.get_entry.return_value": TEST_ENTRY},  # type: ignore
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogGetEntryOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.get_entry.assert_called_once_with(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogGetEntryGroupOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook",
        **{"return_value.get_entry_group.return_value": TEST_ENTRY_GROUP},  # type: ignore
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogGetEntryGroupOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            read_mask=TEST_READ_MASK,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.get_entry_group.assert_called_once_with(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            read_mask=TEST_READ_MASK,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogGetTagTemplateOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook",
        **{"return_value.get_tag_template.return_value": TEST_TAG_TEMPLATE},  # type: ignore
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogGetTagTemplateOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.get_tag_template.assert_called_once_with(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogListTagsOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook",
        **{"return_value.list_tags.return_value": [TEST_TAG]},  # type: ignore
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogListTagsOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            page_size=TEST_PAGE_SIZE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.list_tags.assert_called_once_with(
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            page_size=TEST_PAGE_SIZE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogLookupEntryOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook",
        **{"return_value.lookup_entry.return_value": TEST_ENTRY},  # type: ignore
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogLookupEntryOperator(
            task_id="task_id",
            linked_resource=TEST_LINKED_RESOURCE,
            sql_resource=TEST_SQL_RESOURCE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.lookup_entry.assert_called_once_with(
            linked_resource=TEST_LINKED_RESOURCE,
            sql_resource=TEST_SQL_RESOURCE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogRenameTagTemplateFieldOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook")
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogRenameTagTemplateFieldOperator(
            task_id="task_id",
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            field=TEST_TAG_TEMPLATE_FIELD_ID,
            new_tag_template_field_id=TEST_NEW_TAG_TEMPLATE_FIELD_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.rename_tag_template_field.assert_called_once_with(
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            field=TEST_TAG_TEMPLATE_FIELD_ID,
            new_tag_template_field_id=TEST_NEW_TAG_TEMPLATE_FIELD_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogSearchCatalogOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook")
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogSearchCatalogOperator(
            task_id="task_id",
            scope=TEST_SCOPE,
            query=TEST_QUERY,
            page_size=TEST_PAGE_SIZE,
            order_by=TEST_ORDER_BY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.search_catalog.assert_called_once_with(
            scope=TEST_SCOPE,
            query=TEST_QUERY,
            page_size=TEST_PAGE_SIZE,
            order_by=TEST_ORDER_BY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogUpdateEntryOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook")
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogUpdateEntryOperator(
            task_id="task_id",
            entry=TEST_ENTRY,
            update_mask=TEST_UPDATE_MASK,
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry_id=TEST_ENTRY_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_entry.assert_called_once_with(
            entry=TEST_ENTRY,
            update_mask=TEST_UPDATE_MASK,
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry_id=TEST_ENTRY_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogUpdateTagOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook")
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogUpdateTagOperator(
            task_id="task_id",
            tag=TEST_TAG_ID,
            update_mask=TEST_UPDATE_MASK,
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            tag_id=TEST_TAG_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_tag.assert_called_once_with(
            tag=TEST_TAG_ID,
            update_mask=TEST_UPDATE_MASK,
            location=TEST_LOCATION,
            entry_group=TEST_ENTRY_GROUP_ID,
            entry=TEST_ENTRY_ID,
            tag_id=TEST_TAG_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogUpdateTagTemplateOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook")
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogUpdateTagTemplateOperator(
            task_id="task_id",
            tag_template=TEST_TAG_TEMPLATE_ID,
            update_mask=TEST_UPDATE_MASK,
            location=TEST_LOCATION,
            tag_template_id=TEST_TAG_TEMPLATE_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_tag_template.assert_called_once_with(
            tag_template=TEST_TAG_TEMPLATE_ID,
            update_mask=TEST_UPDATE_MASK,
            location=TEST_LOCATION,
            tag_template_id=TEST_TAG_TEMPLATE_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudDataCatalogUpdateTagTemplateFieldOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogHook")
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudDataCatalogUpdateTagTemplateFieldOperator(
            task_id="task_id",
            tag_template_field=TEST_TAG_TEMPLATE_FIELD,
            update_mask=TEST_UPDATE_MASK,
            tag_template_field_name=TEST_TAG_TEMPLATE_NAME,
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            tag_template_field_id=TEST_TAG_TEMPLATE_FIELD_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_tag_template_field.assert_called_once_with(
            tag_template_field=TEST_TAG_TEMPLATE_FIELD,
            update_mask=TEST_UPDATE_MASK,
            tag_template_field_name=TEST_TAG_TEMPLATE_NAME,
            location=TEST_LOCATION,
            tag_template=TEST_TAG_TEMPLATE_ID,
            tag_template_field_id=TEST_TAG_TEMPLATE_FIELD_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
