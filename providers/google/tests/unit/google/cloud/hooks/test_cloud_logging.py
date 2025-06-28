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

from unittest import mock

import pytest
from google.cloud.logging_v2.types import (
    CreateSinkRequest,
    DeleteSinkRequest,
    GetSinkRequest,
    ListSinksRequest,
    LogSink,
    UpdateSinkRequest,
)

from airflow.providers.google.cloud.hooks.cloud_logging import CloudLoggingHook

from unit.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

PROJECT_ID = "gcp-project-id"
SINK_NAME = "my-logs-sink"
UNQUE_WRITER_IDENTITIY = True
sink_config = {
    "name": SINK_NAME,
    "destination": "storage.googleapis.com/test-log-sink-af",
    "description": "Create with full sink_config",
    "filter": "severity>=INFO",
    "disabled": False,
    "exclusions": [
        {
            "name": "exclude-debug",
            "description": "Skip debug logs",
            "filter": "severity=DEBUG",
            "disabled": True,
        },
        {
            "name": "exclude-cloudsql",
            "description": "Skip CloudSQL logs",
            "filter": 'resource.type="cloudsql_database"',
            "disabled": False,
        },
    ],
}


class TestCloudLoggingHook:
    @pytest.fixture
    def cloud_logging_hook(self):
        hook = CloudLoggingHook()
        return hook

    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials")
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    def test_get_conn_creates_client(self, mock_get_credentials, cloud_logging_hook):
        from google.cloud.logging_v2.services.config_service_v2 import ConfigServiceV2Client

        hook = cloud_logging_hook
        hook._client = None
        conn = hook.get_conn()

        assert isinstance(conn, ConfigServiceV2Client)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_logging.ConfigServiceV2Client")
    def test_create_sink(self, mock_config_client, cloud_logging_hook):
        sink = LogSink(**sink_config)
        expected_request = CreateSinkRequest(
            parent=f"projects/{PROJECT_ID}", sink=sink, unique_writer_identity=UNQUE_WRITER_IDENTITIY
        )

        cloud_logging_hook._client = mock_config_client.return_value

        cloud_logging_hook.create_sink(
            sink=sink, project_id=PROJECT_ID, unique_writer_identity=UNQUE_WRITER_IDENTITIY
        )

        mock_config_client.return_value.create_sink.assert_called_once_with(request=expected_request)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_logging.ConfigServiceV2Client")
    def test_get_sink(self, mock_config_client, cloud_logging_hook):
        expected_request = GetSinkRequest(sink_name=f"projects/{PROJECT_ID}/sinks/{SINK_NAME}")
        cloud_logging_hook._client = mock_config_client.return_value
        cloud_logging_hook.get_sink(sink_name=SINK_NAME, project_id=PROJECT_ID)
        mock_config_client.return_value.get_sink.assert_called_once_with(request=expected_request)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_logging.ConfigServiceV2Client")
    def test_list_sinks(self, mock_config_client, cloud_logging_hook):
        expected_request = ListSinksRequest(parent=f"projects/{PROJECT_ID}")
        cloud_logging_hook._client = mock_config_client.return_value
        cloud_logging_hook.list_sinks(project_id=PROJECT_ID)
        mock_config_client.return_value.list_sinks.assert_called_once_with(request=expected_request)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_logging.ConfigServiceV2Client")
    def test_delete_sink(self, mock_config_client, cloud_logging_hook):
        expected_request = DeleteSinkRequest(sink_name=f"projects/{PROJECT_ID}/sinks/{SINK_NAME}")
        cloud_logging_hook._client = mock_config_client.return_value
        cloud_logging_hook.delete_sink(sink_name=SINK_NAME, project_id=PROJECT_ID)
        mock_config_client.return_value.delete_sink.assert_called_once_with(request=expected_request)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_logging.ConfigServiceV2Client")
    def test_update_sink_success(self, mock_config_client, cloud_logging_hook):
        sink_config = {
            "destination": f"bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/your_dataset",
            "bigquery_options": {"use_partitioned_tables": True},
        }
        update_mask = {"paths": ["destination", "bigquery_options"]}

        hook = CloudLoggingHook()

        expected_request = UpdateSinkRequest(
            sink_name=f"projects/{PROJECT_ID}/sinks/{SINK_NAME}",
            sink=sink_config,
            update_mask=update_mask,
            unique_writer_identity=UNQUE_WRITER_IDENTITIY,
        )

        hook._client = mock_config_client.return_value
        hook.update_sink(
            sink_name=SINK_NAME,
            sink=sink_config,
            update_mask=update_mask,
            unique_writer_identity=UNQUE_WRITER_IDENTITIY,
            project_id=PROJECT_ID,
        )

        mock_config_client.return_value.update_sink.assert_called_once_with(request=expected_request)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_logging.ConfigServiceV2Client")
    def test_create_sink_dict_input(self, mock_config_client, cloud_logging_hook):
        expected_sink = LogSink(**sink_config)
        expected_request = CreateSinkRequest(
            parent=f"projects/{PROJECT_ID}", sink=expected_sink, unique_writer_identity=UNQUE_WRITER_IDENTITIY
        )

        cloud_logging_hook._client = mock_config_client.return_value
        cloud_logging_hook.create_sink(
            sink=sink_config, unique_writer_identity=UNQUE_WRITER_IDENTITIY, project_id=PROJECT_ID
        )

        mock_config_client.return_value.create_sink.assert_called_once_with(request=expected_request)

    def test_update_sink_invalid_dict_format(self, cloud_logging_hook):
        with pytest.raises(ValueError):
            cloud_logging_hook.update_sink(
                sink_name=SINK_NAME,
                sink={"invalid_key": "value"},
                update_mask={"paths": ["invalid_key"]},
                unique_writer_identity=UNQUE_WRITER_IDENTITIY,
                project_id=PROJECT_ID,
            )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_logging.ConfigServiceV2Client")
    def test_update_sink_failure(self, mock_config_client, cloud_logging_hook):
        updated_sink = LogSink(name=SINK_NAME, destination="storage.googleapis.com/new-bucket")
        updated_mask = {"paths": ["name", "destination"]}

        mock_config_client.return_value.update_sink.side_effect = Exception("Permission denied")

        cloud_logging_hook._client = mock_config_client.return_value

        with pytest.raises(Exception, match="Permission denied"):
            cloud_logging_hook.update_sink(
                sink_name=SINK_NAME,
                sink=updated_sink,
                update_mask=updated_mask,
                unique_writer_identity=UNQUE_WRITER_IDENTITIY,
                project_id=PROJECT_ID,
            )

        mock_config_client.return_value.update_sink.assert_called_once()

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_logging.ConfigServiceV2Client")
    def test_list_sinks_empty(self, mock_config_client, cloud_logging_hook):
        mock_config_client.return_value.list_sinks.return_value = []
        cloud_logging_hook._client = mock_config_client.return_value

        sinks = cloud_logging_hook.list_sinks(project_id=PROJECT_ID)

        assert sinks == []
        mock_config_client.return_value.list_sinks.assert_called_once()
