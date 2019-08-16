from typing import Dict, Sequence, Tuple
from unittest import TestCase, mock

from google.api_core.retry import Retry

from tests.contrib.utils.base_gcp_mock import (GCP_PROJECT_ID_HOOK_UNIT_TEST,
                                               mock_base_gcp_hook_default_project_id,
                                               mock_base_gcp_hook_no_default_project_id)

from airflow import AirflowException
from airflow.contrib.hooks.gcp_cloud_memorystore_hook import CloudMemorystoreHook

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_DELEGATE_TO: str = "test-delegate-to"
TEST_LOCATION: str = "test-location"
TEST_INSTANCE_ID: str = "test-instance-id"
TEST_INSTANCE: Dict = None  # TODO: Fill missing value
TEST_PROJECT_ID: str = "test-project-id"
TEST_RETRY: Retry = None  # TODO: Fill missing value
TEST_TIMEOUT: float = None  # TODO: Fill missing value
TEST_METADATA: Sequence[Tuple[str, str]] = None  # TODO: Fill missing value
TEST_PAGE_SIZE: int = None  # TODO: Fill missing value
TEST_UPDATE_MASK: Dict = None  # TODO: Fill missing value
TEST_PARENT: str = "test-parent"
TEST_NAME: str = "test-name"


class TestCloudMemorystoreWithDefaultProjectIdHook(TestCase):
    def setUp(self,):
        with mock.patch(
            "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudMemorystoreHook(gcp_conn_id="test")

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.get_conn"
    )
    def test_create_instance(self, mock_get_conn):
        self.hook.create_instance(
            location=TEST_LOCATION,
            instance_id=TEST_INSTANCE_ID,
            instance=TEST_INSTANCE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.create_instance.assert_called_once_with(
            parent=TEST_PARENT,
            instance_id=TEST_INSTANCE_ID,
            instance=TEST_INSTANCE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.get_conn"
    )
    def test_delete_instance(self, mock_get_conn):
        self.hook.delete_instance(
            location=TEST_LOCATION,
            instance=TEST_INSTANCE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.delete_instance.assert_called_once_with(
            name=TEST_NAME, retry=TEST_RETRY, timeout=TEST_TIMEOUT, metadata=TEST_METADATA
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.get_conn"
    )
    def test_get_instance(self, mock_get_conn):
        self.hook.get_instance(
            location=TEST_LOCATION,
            instance=TEST_INSTANCE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.get_instance.assert_called_once_with(
            name=TEST_NAME, retry=TEST_RETRY, timeout=TEST_TIMEOUT, metadata=TEST_METADATA
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.get_conn"
    )
    def test_list_instances(self, mock_get_conn):
        self.hook.list_instances(
            location=TEST_LOCATION,
            page_size=TEST_PAGE_SIZE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.list_instances.assert_called_once_with(
            parent=TEST_PARENT,
            page_size=TEST_PAGE_SIZE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.get_conn"
    )
    def test_update_instance(self, mock_get_conn):
        self.hook.update_instance(
            update_mask=TEST_UPDATE_MASK,
            instance=TEST_INSTANCE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.update_instance.assert_called_once_with(
            update_mask=TEST_UPDATE_MASK,
            instance=TEST_INSTANCE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudMemorystoreWithoutDefaultProjectIdHook(TestCase):
    def setUp(self,):
        with mock.patch(
            "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = CloudMemorystoreHook(gcp_conn_id="test")

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.get_conn"
    )
    def test_create_instance(self, mock_get_conn):
        self.hook.create_instance(
            location=TEST_LOCATION,
            instance_id=TEST_INSTANCE_ID,
            instance=TEST_INSTANCE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.create_instance.assert_called_once_with(
            parent=TEST_PARENT,
            instance_id=TEST_INSTANCE_ID,
            instance=TEST_INSTANCE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.get_conn"
    )
    def test_create_instance_without_project_id(self, mock_get_conn):
        with self.assertRaises(AirflowException):
            self.hook.create_instance(
                location=TEST_LOCATION,
                instance_id=TEST_INSTANCE_ID,
                instance=TEST_INSTANCE,
                project_id=None,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.get_conn"
    )
    def test_delete_instance(self, mock_get_conn):
        self.hook.delete_instance(
            location=TEST_LOCATION,
            instance=TEST_INSTANCE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.delete_instance.assert_called_once_with(
            name=TEST_NAME, retry=TEST_RETRY, timeout=TEST_TIMEOUT, metadata=TEST_METADATA
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.get_conn"
    )
    def test_delete_instance_without_project_id(self, mock_get_conn):
        with self.assertRaises(AirflowException):
            self.hook.delete_instance(
                location=TEST_LOCATION,
                instance=TEST_INSTANCE,
                project_id=None,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.get_conn"
    )
    def test_get_instance(self, mock_get_conn):
        self.hook.get_instance(
            location=TEST_LOCATION,
            instance=TEST_INSTANCE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.get_instance.assert_called_once_with(
            name=TEST_NAME, retry=TEST_RETRY, timeout=TEST_TIMEOUT, metadata=TEST_METADATA
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.get_conn"
    )
    def test_get_instance_without_project_id(self, mock_get_conn):
        with self.assertRaises(AirflowException):
            self.hook.get_instance(
                location=TEST_LOCATION,
                instance=TEST_INSTANCE,
                project_id=None,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.get_conn"
    )
    def test_list_instances(self, mock_get_conn):
        self.hook.list_instances(
            location=TEST_LOCATION,
            page_size=TEST_PAGE_SIZE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.list_instances.assert_called_once_with(
            parent=TEST_PARENT,
            page_size=TEST_PAGE_SIZE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.get_conn"
    )
    def test_list_instances_without_project_id(self, mock_get_conn):
        with self.assertRaises(AirflowException):
            self.hook.list_instances(
                location=TEST_LOCATION,
                page_size=TEST_PAGE_SIZE,
                project_id=None,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.gcp_cloud_memorystore_hook.CloudMemorystoreHook.get_conn"
    )
    def test_update_instance(self, mock_get_conn):
        self.hook.update_instance(
            update_mask=TEST_UPDATE_MASK,
            instance=TEST_INSTANCE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.update_instance.assert_called_once_with(
            update_mask=TEST_UPDATE_MASK,
            instance=TEST_INSTANCE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
