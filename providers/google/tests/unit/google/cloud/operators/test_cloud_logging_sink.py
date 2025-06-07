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
"""
This module contains various unit tests for GCP Cloud Logging Sink Operators
"""

from __future__ import annotations

from unittest import mock

import pytest
from google.api_core.exceptions import AlreadyExists, GoogleAPICallError, NotFound
from google.cloud.exceptions import GoogleCloudError
from google.cloud.logging_v2.types import BigQueryOptions, LogExclusion, LogSink

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.cloud_logging_sink import (
    CloudLoggingCreateSinkOperator,
    CloudLoggingDeleteSinkOperator,
    CloudLoggingListSinksOperator,
    CloudLoggingUpdateSinkOperator,
)

CLOUD_LOGGING_HOOK_PATH = "airflow.providers.google.cloud.operators.cloud_logging_sink.CloudLoggingHook"
TASK_ID = "test-task"
SINK_NAME = "test-sink"
PROJECT_ID = "test-project"
DESCRIPTION = "testing sink creation"
DESTINATION_PUBSUB = "pubsub.googleapis.com/projects/test-project/topics/test-topic"
DESTINATION_BQ = "bigquery.googleapis.com/projects/test-project/datasets/your_dataset"
FILTER = "severity=ERROR"
EXCLUSION_FILTER = [
    {"name": "exclude-debug", "description": "Skip debug logs", "filter": "severity=DEBUG", "disabled": True},
    {
        "name": "exclude-cloudsql",
        "description": "Skip CloudSQL logs",
        "filter": 'resource.type="cloudsql_database"',
        "disabled": False,
    },
]

BIGQUERY_OPTIONS = {"use_partitioned_tables": True}

SINK_PATH = f"projects/{PROJECT_ID}/sinks/{SINK_NAME}"
UNIQUE_WRITER_IDENTITY = True

SINKS_LIST = [
    {"name": "sink-1", "destination": "storage.googleapis.com/bucket-1"},
    {"name": "sink-2", "destination": "bigquery.googleapis.com/projects/project/datasets/dataset"},
]

# for pub/sub destination
log_sink_pubsub = LogSink()
log_sink_pubsub.name = SINK_NAME
log_sink_pubsub.destination = DESTINATION_PUBSUB
log_sink_pubsub.exclusions = [LogExclusion(**excl) for excl in EXCLUSION_FILTER]

# for bq destination
log_sink_bq = LogSink()
log_sink_bq.name = SINK_NAME
log_sink_bq.destination = DESTINATION_BQ
log_sink_bq.exclusions = [LogExclusion(**excl) for excl in EXCLUSION_FILTER]
log_sink_bq.bigquery_options = BigQueryOptions(**BIGQUERY_OPTIONS)


def _assert_common_template_fields(template_fields):
    assert "project_id" in template_fields
    assert "gcp_conn_id" in template_fields
    assert "impersonation_chain" in template_fields


class TestCloudLoggingCreateSinkOperator:
    def test_template_fields(self):
        operator = CloudLoggingCreateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            destination=DESTINATION_BQ,
            filter_=FILTER,
            exclusion_filter=EXCLUSION_FILTER,
            project_id=PROJECT_ID,
            description=DESCRIPTION,
        )
        assert "sink_name" in operator.template_fields
        assert "filter_" in operator.template_fields
        assert "exclusion_filter" in operator.template_fields
        assert "description" in operator.template_fields
        _assert_common_template_fields(operator.template_fields)

    def test_missing_required_params(self):
        with pytest.raises(AirflowException) as excinfo:
            CloudLoggingCreateSinkOperator(
                task_id=TASK_ID,
                sink_name=None,
                destination=None,
                project_id=None,
            )
        assert "Required parameters are missing" in str(excinfo.value)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_create_with_pubsub_sink(self, hook_mock):
        client_mock = mock.MagicMock()
        client_mock.create_sink.return_value = log_sink_pubsub

        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingCreateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            destination=DESTINATION_PUBSUB,
            exclusion_filter=EXCLUSION_FILTER,
            project_id=PROJECT_ID,
        )

        operator.execute(context=mock.MagicMock())

        client_mock.create_sink.assert_called_once_with(
            request={
                "parent": f"projects/{PROJECT_ID}",
                "sink": log_sink_pubsub,
                "unique_writer_identity": UNIQUE_WRITER_IDENTITY,
            }
        )

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_create_with_bq_sink(self, hook_mock):
        client_mock = mock.MagicMock()
        client_mock.create_sink.return_value = log_sink_bq

        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingCreateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            exclusion_filter=EXCLUSION_FILTER,
            bigquery_options=BIGQUERY_OPTIONS,
            destination=DESTINATION_BQ,
            project_id=PROJECT_ID,
        )

        operator.execute(context=mock.MagicMock())

        client_mock.create_sink.assert_called_once_with(
            request={
                "parent": f"projects/{PROJECT_ID}",
                "sink": log_sink_bq,
                "unique_writer_identity": UNIQUE_WRITER_IDENTITY,
            }
        )

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_create_sink_already_exists(self, hook_mock):
        existing_sink = log_sink_bq

        client_mock = mock.MagicMock()
        client_mock.create_sink.side_effect = AlreadyExists("Sink already exists")
        client_mock.get_sink.return_value = existing_sink
        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingCreateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            exclusion_filter=EXCLUSION_FILTER,
            bigquery_options=BIGQUERY_OPTIONS,
            destination=DESTINATION_BQ,
            project_id=PROJECT_ID,
        )

        result = operator.execute(context=mock.MagicMock())

        client_mock.create_sink.assert_called_once()
        client_mock.get_sink.assert_called_once()

        assert result == LogSink.to_dict(existing_sink)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_create_sink_raises_error(self, hook_mock):
        client_mock = mock.MagicMock()
        client_mock.create_sink.side_effect = GoogleAPICallError("Failed to create sink")
        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingCreateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            destination=DESTINATION_BQ,
            project_id=PROJECT_ID,
        )

        with pytest.raises(GoogleAPICallError, match="Failed to create sink"):
            operator.execute(context=mock.MagicMock())

        client_mock.create_sink.assert_called_once()

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_create_with_impersonation_chain(self, hook_mock):
        client_mock = mock.MagicMock()
        client_mock.create_sink.return_value = log_sink_bq

        hook_mock.return_value.get_conn.return_value = client_mock

        impersonation_chain = ["user1@project.iam.gserviceaccount.com"]

        operator = CloudLoggingCreateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            destination=DESTINATION_BQ,
            project_id=PROJECT_ID,
            impersonation_chain=impersonation_chain,
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.assert_called_once_with(
            gcp_conn_id="google_cloud_default", impersonation_chain=impersonation_chain
        )

    def test_create_with_empty_sink_name_raises(self):
        with pytest.raises(
            AirflowException,
            match=r"Required parameters are missing: \['sink_name'\]\. These parameters must be passed as keyword parameters or as extra fields in Airflow connection definition\.",
        ):
            CloudLoggingCreateSinkOperator(
                task_id=TASK_ID, sink_name="", destination=DESTINATION_BQ, project_id=PROJECT_ID
            )


class TestCloudLoggingDeleteSinkOperator:
    def test_template_fields(self):
        operator = CloudLoggingDeleteSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
        )
        assert "sink_name" in operator.template_fields
        _assert_common_template_fields(operator.template_fields)

    def test_missing_required_params(self):
        with pytest.raises(AirflowException) as excinfo:
            CloudLoggingDeleteSinkOperator(
                task_id=TASK_ID,
                sink_name=None,
                project_id=None,
            )
        assert "Required parameters are missing" in str(excinfo.value)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_delete_sink_success(self, hook_mock):
        client_mock = mock.MagicMock()
        log_sink = LogSink(name=SINK_NAME, destination=DESTINATION_BQ)
        client_mock.get_sink.return_value = log_sink
        client_mock.delete_sink.return_value = None
        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingDeleteSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
        )

        context = mock.MagicMock()
        result = operator.execute(context=context)

        client_mock.get_sink.assert_called_once()
        client_mock.delete_sink.assert_called_once()

        assert result == LogSink.to_dict(log_sink)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_delete_sink_raises_error(self, hook_mock):
        client_mock = mock.MagicMock()
        client_mock.get_sink.side_effect = GoogleCloudError("Internal error")

        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingDeleteSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
        )

        with pytest.raises(GoogleCloudError):
            operator.execute(context=mock.MagicMock())

        client_mock.get_sink.assert_called_once()
        client_mock.delete_sink.assert_not_called()


class TestCloudLoggingListSinksOperator:
    def test_template_fields(self):
        operator = CloudLoggingListSinksOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
        )
        assert "project_id" in operator.template_fields

    def test_missing_required_params(self):
        with pytest.raises(AirflowException) as excinfo:
            CloudLoggingListSinksOperator(
                task_id=TASK_ID,
                project_id=None,
            )
        assert "Required parameter 'project_id' is missing." in str(excinfo.value)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_list_sinks_success(self, hook_mock):
        client_mock = mock.MagicMock()
        sink1 = LogSink(name="sink-1", destination="storage.googleapis.com/bucket-1")
        sink2 = LogSink(
            name="sink-2", destination="bigquery.googleapis.com/projects/project/datasets/dataset"
        )
        client_mock.list_sinks.return_value = [sink1, sink2]

        hook_instance_mock = hook_mock.return_value
        hook_instance_mock.get_conn.return_value = client_mock

        operator = CloudLoggingListSinksOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            page_size=50,
        )
        result = operator.execute(context=mock.MagicMock())
        hook_mock.assert_called_once()
        _, kwargs = hook_mock.call_args
        assert kwargs == {
            "gcp_conn_id": "google_cloud_default",
            "impersonation_chain": None,
        }

        client_mock.list_sinks.assert_called_once_with(
            request={"parent": f"projects/{PROJECT_ID}", "page_size": "50"}
        )

        assert result == [LogSink.to_dict(sink) for sink in [sink1, sink2]]

    def test_negative_page_size_raises_exception(self):
        with pytest.raises(
            AirflowException, match="page_size for the list sinks request should be greater or equal to zero"
        ):
            CloudLoggingListSinksOperator(task_id="fail-task", project_id=PROJECT_ID, page_size=-1)


class TestCloudLoggingUpdateSinksOperator:
    def test_template_fields(self):
        operator = CloudLoggingCreateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            destination=DESTINATION_BQ,
            filter_=FILTER,
            exclusion_filter=EXCLUSION_FILTER,
            project_id=PROJECT_ID,
            description=DESCRIPTION,
        )
        assert "sink_name" in operator.template_fields
        assert "filter_" in operator.template_fields
        assert "exclusion_filter" in operator.template_fields
        assert "description" in operator.template_fields
        _assert_common_template_fields(operator.template_fields)

    def test_missing_required_params(self):
        with pytest.raises(AirflowException) as excinfo:
            CloudLoggingDeleteSinkOperator(
                task_id=TASK_ID,
                sink_name=None,
                project_id=None,
            )
        assert "Required parameters are missing" in str(excinfo.value)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_update_sink_success(self, hook_mock):
        client_mock = mock.MagicMock()
        existing_sink = LogSink(name=SINK_NAME)
        client_mock.get_sink.return_value = existing_sink
        client_mock.update_sink.return_value = existing_sink

        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingUpdateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
            destination=DESTINATION_BQ,
            filter_="severity>=ERROR",
            exclusion_filter=EXCLUSION_FILTER,
            bigquery_options=BIGQUERY_OPTIONS,
            description="updated",
            disabled=True,
            include_children=True,
        )

        result = operator.execute(context=mock.MagicMock())

        client_mock.get_sink.assert_called_once_with(
            request={"sink_name": f"projects/{PROJECT_ID}/sinks/{SINK_NAME}"}
        )
        client_mock.update_sink.assert_called_once()
        assert result["name"] == SINK_NAME

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_update_sink_raises_not_found(self, hook_mock):
        client_mock = mock.MagicMock()
        client_mock.get_sink.side_effect = NotFound("not found")
        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingUpdateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
        )

        with pytest.raises(NotFound, match="not found"):
            operator.execute(context=mock.MagicMock())

        client_mock.get_sink.assert_called_once()
        client_mock.update_sink.assert_not_called()

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_update_sink_raises_generic_error(self, hook_mock):
        client_mock = mock.MagicMock()
        client_mock.get_sink.side_effect = GoogleAPICallError("something went wrong")
        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingUpdateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
        )

        with pytest.raises(GoogleAPICallError, match="something went wrong"):
            operator.execute(context=mock.MagicMock())

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_update_with_impersonation_chain(self, hook_mock):
        impersonation_chain = ["user1@project.iam.gserviceaccount.com"]
        client_mock = mock.MagicMock()
        sink = LogSink(name=SINK_NAME)
        client_mock.get_sink.return_value = sink
        client_mock.update_sink.return_value = sink
        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingUpdateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
            destination=DESTINATION_BQ,
            impersonation_chain=impersonation_chain,
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.assert_called_once_with(
            gcp_conn_id="google_cloud_default", impersonation_chain=impersonation_chain
        )

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_update_sink_with_no_fields_warns(self, hook_mock):
        client_mock = mock.MagicMock()
        sink = LogSink(name=SINK_NAME)
        client_mock.get_sink.return_value = sink
        client_mock.update_sink.return_value = sink
        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingUpdateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
        )

        result = operator.execute(context=mock.MagicMock())

        client_mock.get_sink.assert_called_once()
        client_mock.update_sink.assert_not_called()
        assert result == LogSink.to_dict(sink)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_update_sink_only_description(self, hook_mock):
        client_mock = mock.MagicMock()
        sink = LogSink(name=SINK_NAME)
        client_mock.get_sink.return_value = sink
        client_mock.update_sink.return_value = sink
        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingUpdateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
            description="new description",
        )

        operator.execute(context=mock.MagicMock())
        update_mask = client_mock.update_sink.call_args[1]["request"]["update_mask"]["paths"]
        assert update_mask == ["description"]
