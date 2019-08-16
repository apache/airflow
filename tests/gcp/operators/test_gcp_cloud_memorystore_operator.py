from typing import Dict, Sequence, Tuple
from unittest import TestCase, mock

from google.api_core.retry import Retry
from google.cloud.redis_v1.types import DataProtectionMode, FieldMask, InputConfig, Instance, OutputConfig

from airflow.gcp.operators.cloud_memorystore import (CloudMemorystoreCreateInstanceOperator,
                                                     CloudMemorystoreDeleteInstanceOperator,
                                                     CloudMemorystoreExportInstanceOperator,
                                                     CloudMemorystoreFailoverInstanceOperator,
                                                     CloudMemorystoreGetInstanceOperator,
                                                     CloudMemorystoreImportInstanceOperator,
                                                     CloudMemorystoreListInstancesOperator,
                                                     CloudMemorystoreUpdateInstanceOperator)


TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_TASK_ID: str = 'task-id'
TEST_LOCATION: str = "test-location"
TEST_INSTANCE_ID: str = "test-instance-id"
TEST_INSTANCE: Dict = None  # TODO: Fill missing value
TEST_INSTANCE_NAME: str = "test-instance-name"
TEST_PROJECT_ID: str = "test-project-id"
TEST_RETRY: Retry = None  # TODO: Fill missing value
TEST_TIMEOUT: float = None  # TODO: Fill missing value
TEST_METADATA: Sequence[Tuple[str, str]] = None  # TODO: Fill missing value
TEST_OUTPUT_CONFIG: Dict = None  # TODO: Fill missing value
TEST_DATA_PROTECTION_MODE: DataProtectionMode = None  # TODO: Fill missing value
TEST_INPUT_CONFIG: Dict = None  # TODO: Fill missing value
TEST_PAGE_SIZE: int = None  # TODO: Fill missing value
TEST_UPDATE_MASK: Dict = None  # TODO: Fill missing value
TEST_PARENT: str = "test-parent"
TEST_NAME: str = "test-name"


class TestCloudMemorystoreCreateInstanceOperator(TestCase):
    @mock.patch("airflow.gcp.operators.cloud_memorystore.CloudMemorystoreHook")
    def test_assert_valid_hook_call(self, mock_hook):
        task = CloudMemorystoreCreateInstanceOperator(
            task_id=TEST_TASK_ID,
            location=TEST_LOCATION,
            instance_id=TEST_INSTANCE_ID,
            instance=TEST_INSTANCE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.create_instance.assert_called_once_with(
            location=TEST_LOCATION,
            instance_id=TEST_INSTANCE_ID,
            instance=TEST_INSTANCE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudMemorystoreDeleteInstanceOperator(TestCase):
    @mock.patch("airflow.gcp.operators.cloud_memorystore.CloudMemorystoreHook")
    def test_assert_valid_hook_call(self, mock_hook):
        task = CloudMemorystoreDeleteInstanceOperator(
            task_id=TEST_TASK_ID,
            location=TEST_LOCATION,
            instance=TEST_INSTANCE_NAME,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.delete_instance.assert_called_once_with(
            location=TEST_LOCATION,
            instance=TEST_INSTANCE_NAME,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudMemorystoreExportInstanceOperator(TestCase):
    @mock.patch("airflow.contrib.operator.gcp_cloud_memorystore_operator.CloudMemorystoreHook")
    def test_assert_valid_hook_call(self, mock_hook):
        task = CloudMemorystoreExportInstanceOperator(
            task_id=TEST_TASK_ID,
            location=TEST_LOCATION,
            instance=TEST_INSTANCE_NAME,
            output_config=TEST_OUTPUT_CONFIG,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.export_instance.assert_called_once_with(
            location=TEST_LOCATION,
            instance=TEST_INSTANCE,
            output_config=TEST_OUTPUT_CONFIG,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudMemorystoreFailoverInstanceOperator(TestCase):
    @mock.patch("airflow.contrib.operator.gcp_cloud_memorystore_operator.CloudMemorystoreHook")
    def test_assert_valid_hook_call(self, mock_hook):
        task = CloudMemorystoreFailoverInstanceOperator(
            task_id=TEST_TASK_ID,
            location=TEST_LOCATION,
            instance=TEST_INSTANCE_NAME,
            data_protection_mode=TEST_DATA_PROTECTION_MODE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.failover_instance.assert_called_once_with(
            location=TEST_LOCATION,
            instance=TEST_INSTANCE,
            data_protection_mode=TEST_DATA_PROTECTION_MODE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudMemorystoreGetInstanceOperator(TestCase):
    @mock.patch("airflow.gcp.operators.cloud_memorystore.CloudMemorystoreHook")
    def test_assert_valid_hook_call(self, mock_hook):
        task = CloudMemorystoreGetInstanceOperator(
            task_id=TEST_TASK_ID,
            location=TEST_LOCATION,
            instance=TEST_INSTANCE_NAME,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.get_instance.assert_called_once_with(
            location=TEST_LOCATION,
            instance=TEST_INSTANCE_NAME,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudMemorystoreImportInstanceOperator(TestCase):
    @mock.patch("airflow.contrib.operator.gcp_cloud_memorystore_operator.CloudMemorystoreHook")
    def test_assert_valid_hook_call(self, mock_hook):
        task = CloudMemorystoreImportInstanceOperator(
            task_id=TEST_TASK_ID,
            location=TEST_LOCATION,
            instance=TEST_INSTANCE_NAME,
            input_config=TEST_INPUT_CONFIG,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.import_instance.assert_called_once_with(
            location=TEST_LOCATION,
            instance=TEST_INSTANCE,
            input_config=TEST_INPUT_CONFIG,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudMemorystoreListInstancesOperator(TestCase):
    @mock.patch("airflow.gcp.operators.cloud_memorystore.CloudMemorystoreHook")
    def test_assert_valid_hook_call(self, mock_hook):
        task = CloudMemorystoreListInstancesOperator(
            task_id=TEST_TASK_ID,
            location=TEST_LOCATION,
            page_size=TEST_PAGE_SIZE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.list_instances.assert_called_once_with(
            location=TEST_LOCATION,
            page_size=TEST_PAGE_SIZE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudMemorystoreUpdateInstanceOperator(TestCase):
    @mock.patch("airflow.gcp.operators.cloud_memorystore.CloudMemorystoreHook")
    def test_assert_valid_hook_call(self, mock_hook):
        task = CloudMemorystoreUpdateInstanceOperator(
            task_id=TEST_TASK_ID,
            update_mask=TEST_UPDATE_MASK,
            instance=TEST_INSTANCE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.update_instance.assert_called_once_with(
            update_mask=TEST_UPDATE_MASK,
            instance=TEST_INSTANCE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
