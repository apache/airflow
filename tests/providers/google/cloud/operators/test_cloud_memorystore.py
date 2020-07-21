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
from typing import Dict, Sequence, Tuple
from unittest import TestCase, mock

from google.api_core.retry import Retry
from google.cloud.redis_v1.gapic.enums import FailoverInstanceRequest
from google.cloud.redis_v1.types import Instance

from airflow.providers.google.cloud.operators.cloud_memorystore import (
    CloudMemorystoreCreateInstanceAndImportOperator, CloudMemorystoreCreateInstanceOperator,
    CloudMemorystoreDeleteInstanceOperator, CloudMemorystoreExportInstanceOperator,
    CloudMemorystoreFailoverInstanceOperator, CloudMemorystoreGetInstanceOperator,
    CloudMemorystoreImportOperator, CloudMemorystoreListInstancesOperator,
    CloudMemorystoreScaleInstanceOperator, CloudMemorystoreUpdateInstanceOperator,
)

TEST_GCP_CONN_ID = "test-gcp-conn-id"
TEST_TASK_ID = "task-id"
TEST_LOCATION = "test-location"
TEST_INSTANCE_ID = "test-instance-id"
TEST_INSTANCE = Instance(name="instance")
TEST_INSTANCE_NAME = "test-instance-name"
TEST_PROJECT_ID = "test-project-id"
TEST_RETRY = Retry()  # type: Retry
TEST_TIMEOUT = 10  # type: float
TEST_INSTANCE_SIZE = 4  # type: int
TEST_METADATA = [("KEY", "VALUE")]  # type: Sequence[Tuple[str, str]]
TEST_OUTPUT_CONFIG = {"gcs_destination": {"uri": "gs://test-bucket/file.rdb"}}  # type: Dict
TEST_DATA_PROTECTION_MODE = FailoverInstanceRequest.DataProtectionMode.LIMITED_DATA_LOSS
TEST_INPUT_CONFIG = {"gcs_source": {"uri": "gs://test-bucket/file.rdb"}}  # type: Dict
TEST_PAGE_SIZE = 100  # type: int
TEST_UPDATE_MASK = {"paths": ["memory_size_gb"]}  # TODO: Fill missing value
TEST_PARENT = "test-parent"
TEST_NAME = "test-name"


class TestCloudMemorystoreCreateInstanceOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreHook")
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
    @mock.patch("airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreHook")
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
    @mock.patch("airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreHook")
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
            instance=TEST_INSTANCE_NAME,
            output_config=TEST_OUTPUT_CONFIG,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudMemorystoreFailoverInstanceOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreHook")
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
            instance=TEST_INSTANCE_NAME,
            data_protection_mode=TEST_DATA_PROTECTION_MODE,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudMemorystoreGetInstanceOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreHook")
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


class TestCloudMemorystoreImportOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreHook")
    def test_assert_valid_hook_call(self, mock_hook):
        task = CloudMemorystoreImportOperator(
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
            instance=TEST_INSTANCE_NAME,
            input_config=TEST_INPUT_CONFIG,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudMemorystoreListInstancesOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreHook")
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
    @mock.patch("airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreHook")
    def test_assert_valid_hook_call(self, mock_hook):
        task = CloudMemorystoreUpdateInstanceOperator(
            task_id=TEST_TASK_ID,
            update_mask=TEST_UPDATE_MASK,
            instance=TEST_INSTANCE,
            location=TEST_LOCATION,
            instance_id=TEST_INSTANCE_ID,
            project_id=TEST_PROJECT_ID,
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
            location=TEST_LOCATION,
            instance_id=TEST_INSTANCE_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudMemorystoreScaleInstanceOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreHook")
    def test_assert_valid_hook_call(self, mock_hook):
        task = CloudMemorystoreScaleInstanceOperator(
            task_id=TEST_TASK_ID,
            memory_size_gb=TEST_INSTANCE_SIZE,
            location=TEST_LOCATION,
            instance_id=TEST_INSTANCE_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.update_instance.assert_called_once_with(
            update_mask={"paths": ["memory_size_gb"]},
            instance={"memory_size_gb": 4},
            location=TEST_LOCATION,
            instance_id=TEST_INSTANCE_ID,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudMemorystoreCreateInstanceAndImportOperatorOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreHook")
    def test_assert_valid_hook_call(self, mock_hook):
        task = CloudMemorystoreCreateInstanceAndImportOperator(
            task_id=TEST_TASK_ID,
            location=TEST_LOCATION,
            instance_id=TEST_INSTANCE_ID,
            instance=TEST_INSTANCE,
            input_config=TEST_INPUT_CONFIG,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(mock.MagicMock())
        mock_hook.assert_has_calls(
            [
                mock.call(gcp_conn_id=TEST_GCP_CONN_ID),
                mock.call().create_instance(
                    location=TEST_LOCATION,
                    instance_id=TEST_INSTANCE_ID,
                    instance=TEST_INSTANCE,
                    project_id=TEST_PROJECT_ID,
                    retry=TEST_RETRY,
                    timeout=TEST_TIMEOUT,
                    metadata=TEST_METADATA,
                ),
                mock.call().import_instance(
                    input_config=TEST_INPUT_CONFIG,
                    instance=TEST_INSTANCE_ID,
                    location=TEST_LOCATION,
                    metadata=TEST_METADATA,
                    project_id=TEST_PROJECT_ID,
                    retry=TEST_RETRY,
                    timeout=TEST_TIMEOUT,
                ),
            ]
        )
