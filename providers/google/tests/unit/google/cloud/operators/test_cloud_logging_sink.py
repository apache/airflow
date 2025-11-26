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

import re
from datetime import datetime
from unittest import mock

import pytest
from google.api_core.exceptions import AlreadyExists, GoogleAPICallError, InvalidArgument, NotFound
from google.cloud.exceptions import GoogleCloudError
from google.cloud.logging_v2.types import LogSink
from google.protobuf.field_mask_pb2 import FieldMask

from airflow import DAG
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
UNIQUE_WRITER_IDENTITY = True

create_test_cases = [
    (
        {
            "name": SINK_NAME,
            "description": "Creating sink with pubsub",
            "destination": "pubsub.googleapis.com/projects/test-project/topics/test-topic",
            "filter": "severity=INFO",
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
    ),
    (
        {
            "name": SINK_NAME,
            "description": "Creating bq destination",
            "destination": "bigquery.googleapis.com/projects/test-project/datasets/your_dataset",
            "filter": "severity=ERROR",
            "exclusions": [
                {
                    "name": "exclude-healthchecks",
                    "description": "Exclude App Engine health check logs",
                    "filter": 'resource.type="gae_app" AND protoPayload.status.code=200 AND protoPayload.resource="/_ah/health"',
                    "disabled": False,
                },
                {
                    "name": "exclude-load-balancer-logs",
                    "description": "Exclude HTTP 200 logs from load balancer",
                    "filter": 'resource.type="http_load_balancer" AND httpRequest.status=200',
                    "disabled": False,
                },
                {
                    "name": "exclude-gke-events",
                    "description": "Exclude normal Kubernetes events",
                    "filter": 'resource.type="k8s_event" AND jsonPayload.reason="Scheduled"',
                    "disabled": False,
                },
            ],
            "bigquery_options": {"use_partitioned_tables": True},
        }
    ),
]
create_test_ids = ["create_pubsub", "create_bq"]

update_test_cases = [
    (
        {
            "name": "sink-1",
            "destination": "storage.googleapis.com/my-bucket-1",
            "filter": "severity>=ERROR",
            "description": "Storage sink updated",
            "disabled": False,
        },
        {"paths": ["filter", "description", "disabled"]},
    ),
    (
        {
            "name": "sink-2",
            "destination": "pubsub.googleapis.com/projects/my-project/topics/my-topic",
            "filter": 'resource.type="gce_instance"',
            "description": "Pub/Sub sink updated",
            "disabled": True,
        },
        {"paths": ["destination", "disabled"]},
    ),
]

update_test_ids = ["update_storage_sink", "update_pubsub_sink"]


sink = LogSink(name=SINK_NAME, destination="pubsub.googleapis.com/projects/my-project/topics/my-topic")


def _assert_common_template_fields(template_fields):
    assert "project_id" in template_fields
    assert "gcp_conn_id" in template_fields
    assert "impersonation_chain" in template_fields


class TestCloudLoggingCreateSinkOperator:
    def test_template_fields(self):
        operator = CloudLoggingCreateSinkOperator(task_id=TASK_ID, project_id=PROJECT_ID, sink_config=sink)
        assert "sink_config" in operator.template_fields
        assert "unique_writer_identity" in operator.template_fields
        _assert_common_template_fields(operator.template_fields)

    def test_missing_required_params(self):
        with pytest.raises(AirflowException) as excinfo:
            CloudLoggingCreateSinkOperator(
                task_id=TASK_ID,
                sink_config=None,
                project_id=None,
            ).execute(context={})
        assert "Required parameters are missing: ['project_id', 'sink_config']." in str(excinfo.value)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    @pytest.mark.parametrize("sink_config", create_test_cases, ids=create_test_ids)
    def test_create_with_pubsub_sink(self, hook_mock, sink_config):
        hook_instance = hook_mock.return_value
        hook_instance.create_sink.return_value = LogSink(**sink_config)

        operator = CloudLoggingCreateSinkOperator(
            task_id=TASK_ID,
            sink_config=sink_config,
            project_id=PROJECT_ID,
            unique_writer_identity=UNIQUE_WRITER_IDENTITY,
        )

        operator.execute(context=mock.MagicMock())

        hook_instance.create_sink.assert_called_once_with(
            project_id=PROJECT_ID, sink=sink_config, unique_writer_identity=UNIQUE_WRITER_IDENTITY
        )

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    @pytest.mark.parametrize("sink_config", create_test_cases, ids=create_test_ids)
    def test_create_sink_already_exists(self, hook_mock, sink_config):
        hook_instance = hook_mock.return_value
        hook_instance.create_sink.side_effect = AlreadyExists("Sink already exists")
        hook_instance.get_sink.return_value = LogSink(**sink_config)

        operator = CloudLoggingCreateSinkOperator(
            task_id=TASK_ID,
            sink_config=sink_config,
            project_id=PROJECT_ID,
        )

        result = operator.execute(context=mock.MagicMock())

        hook_instance.create_sink.assert_called_once_with(
            project_id=PROJECT_ID, sink=sink_config, unique_writer_identity=False
        )
        assert result["name"] == sink_config["name"]
        assert result["destination"] == sink_config["destination"]

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    @pytest.mark.parametrize("sink_config", create_test_cases, ids=create_test_ids)
    def test_create_sink_raises_error(self, hook_mock, sink_config):
        hook_instance = hook_mock.return_value
        hook_instance.create_sink.side_effect = GoogleCloudError("Failed to create sink")
        hook_instance.get_sink.return_value = sink_config

        operator = CloudLoggingCreateSinkOperator(
            task_id=TASK_ID,
            sink_config=sink_config,
            project_id=PROJECT_ID,
        )

        with pytest.raises(GoogleCloudError, match="Failed to create sink"):
            operator.execute(context=mock.MagicMock())

        hook_instance.create_sink.assert_called_once()

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    @pytest.mark.parametrize(
        "impersonation_chain",
        [
            ["user1@project.iam.gserviceaccount.com", "user2@project.iam.gserviceaccount.com"],
            "user2@project.iam.gserviceaccount.com",
        ],
    )
    def test_create_with_impersonation_chain(self, hook_mock, impersonation_chain):
        hook_instance = hook_mock.return_value
        hook_instance.create_sink.return_value = sink

        operator = CloudLoggingCreateSinkOperator(
            task_id=TASK_ID,
            sink_config=sink,
            impersonation_chain=impersonation_chain,
            project_id=PROJECT_ID,
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=impersonation_chain,
        )

    def test_missing_rendered_field_raises(self):
        with DAG(
            dag_id="test_render_native",
            start_date=datetime(1997, 9, 25),
            render_template_as_native_obj=True,
        ) as dag:
            operator = CloudLoggingCreateSinkOperator(
                task_id=TASK_ID,
                sink_config="{{ var.value.sink_config }}",
                project_id="{{ var.value.project_id }}",
                dag=dag,
            )

        context = {
            "var": {"value": {"project_id": PROJECT_ID, "sink_config": None}},
        }

        operator.render_template_fields(context)

        with pytest.raises(
            AirflowException,
            match=re.escape(
                "Required parameters are missing: ['sink_config']. These must be passed as keyword parameters."
            ),
        ):
            operator.execute(context)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    @pytest.mark.parametrize("sink_config", create_test_cases, ids=create_test_ids)
    def test_template_rendering(self, hook_mock, sink_config):
        with DAG(
            dag_id="test_render_native",
            start_date=datetime(2024, 1, 1),
            render_template_as_native_obj=True,
        ) as dag:
            operator = CloudLoggingCreateSinkOperator(
                task_id=TASK_ID,
                sink_config="{{ var.value.sink_config }}",
                project_id="{{ var.value.project_id }}",
                dag=dag,
            )

        context = {
            "var": {"value": {"project_id": PROJECT_ID, "sink_config": sink_config}},
        }

        hook_instance = hook_mock.return_value
        hook_instance.create_sink.return_value = LogSink(**sink_config)

        operator.render_template_fields(context)

        operator.execute(context)

        assert isinstance(operator.sink_config, dict)
        assert operator.sink_config == sink_config

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_create_with_empty_sink_name_raises(self, hook_mock):
        sink.name = None
        hook_instance = hook_mock.return_value
        hook_instance.create_sink.side_effect = InvalidArgument("Required parameter 'sink.name' is empty")
        with pytest.raises(
            InvalidArgument,
            match="400 Required parameter 'sink.name' is empty",
        ):
            CloudLoggingCreateSinkOperator(task_id=TASK_ID, sink_config=sink, project_id=PROJECT_ID).execute(
                context={}
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
            ).execute(context={})
        assert "Required parameters are missing" in str(excinfo.value)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_delete_sink_success(self, hook_mock):
        hook_instance = hook_mock.return_value
        hook_instance.delete_sink.return_value = None
        operator = CloudLoggingDeleteSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
        )

        context = mock.MagicMock()
        operator.execute(context=context)

        hook_instance.delete_sink.assert_called_once()

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_delete_sink_raises_error(self, hook_mock):
        hook_instance = hook_mock.return_value
        hook_instance.delete_sink.side_effect = GoogleCloudError("Internal Error")

        operator = CloudLoggingDeleteSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
        )

        with pytest.raises(GoogleCloudError):
            operator.execute(context=mock.MagicMock())

        hook_instance.delete_sink.assert_called_once()

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_missing_rendered_field_raises(self, hook_mock):
        with DAG(
            dag_id="test_render_native",
            start_date=datetime(2024, 1, 1),
            render_template_as_native_obj=True,
        ) as dag:
            operator = CloudLoggingDeleteSinkOperator(
                task_id=TASK_ID,
                sink_name="{{ var.value.sink_name }}",
                project_id="{{ var.value.project_id }}",
                dag=dag,
            )

        context = {
            "var": {"value": {"project_id": PROJECT_ID, "sink_name": None}},
        }

        operator.render_template_fields(context)
        with pytest.raises(
            AirflowException,
            match=re.escape(
                "Required parameters are missing: ['sink_name']. These must be passed as keyword parameters."
            ),
        ):
            operator.execute(context)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    @pytest.mark.parametrize("sink_config", create_test_cases, ids=create_test_ids)
    def test_template_rendering(self, hook_mock, sink_config):
        with DAG(
            dag_id="test_render_native",
            start_date=datetime(2024, 1, 1),
            render_template_as_native_obj=True,
        ) as dag:
            operator = CloudLoggingDeleteSinkOperator(
                task_id=TASK_ID,
                sink_name="{{ var.value.sink_name }}",
                project_id="{{ var.value.project_id }}",
                dag=dag,
            )

        context = {
            "var": {"value": {"project_id": PROJECT_ID, "sink_name": SINK_NAME}},
        }

        hook_instance = hook_mock.return_value
        hook_instance.delete_sink.return_value = None

        operator.render_template_fields(context)

        operator.execute(context)

        assert operator.project_id == PROJECT_ID
        assert operator.sink_name == SINK_NAME


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
            ).execute(context={})
        assert "Required parameters are missing" in str(excinfo.value)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_list_sinks_success(self, hook_mock):
        hook_instance = hook_mock.return_value
        hook_instance.list_sinks.return_value = [sink, sink]
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

        hook_instance.list_sinks.assert_called_once_with(
            project_id=PROJECT_ID,
            page_size=50,
        )

        assert result == [LogSink.to_dict(sink) for sink in [sink, sink]]

    def test_negative_page_size_raises_exception(self):
        with pytest.raises(
            AirflowException, match="The page_size for the list sinks request must be greater than zero"
        ):
            CloudLoggingListSinksOperator(task_id="fail-task", project_id=PROJECT_ID, page_size=-1).execute(
                context={}
            )

    def test_missing_rendered_field_raises(self):
        with DAG(
            dag_id="test_render_native",
            start_date=datetime(1997, 9, 25),
            render_template_as_native_obj=True,
        ) as dag:
            operator = CloudLoggingListSinksOperator(
                task_id=TASK_ID, project_id="{{ var.value.project_id }}", dag=dag
            )

        context = {
            "var": {"value": {"project_id": None}},
        }

        operator.render_template_fields(context)

        with pytest.raises(
            AirflowException,
            match=re.escape(
                "Required parameters are missing: ['project_id']. These must be passed as keyword parameters."
            ),
        ):
            operator.execute(context)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_template_rendering(self, hook_mock):
        with DAG(
            dag_id="test_render_native",
            start_date=datetime(2024, 1, 1),
            render_template_as_native_obj=True,
        ) as dag:
            operator = CloudLoggingListSinksOperator(
                task_id=TASK_ID, project_id="{{ var.value.project_id }}", dag=dag
            )

        context = {
            "var": {"value": {"project_id": PROJECT_ID}},
        }

        hook_instance = hook_mock.return_value
        hook_instance.list_sinks.return_value = [sink]

        operator.render_template_fields(context)

        operator.execute(context)

        assert isinstance(operator.project_id, str)
        assert operator.project_id == PROJECT_ID


class TestCloudLoggingUpdateSinksOperator:
    @pytest.mark.parametrize(("sink_config", "update_mask"), update_test_cases, ids=update_test_ids)
    def test_template_fields(self, sink_config, update_mask):
        operator = CloudLoggingUpdateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            sink_config=sink_config,
            update_mask=update_mask,
            project_id=PROJECT_ID,
        )
        assert "sink_config" in operator.template_fields
        assert "update_mask" in operator.template_fields
        assert "sink_name" in operator.template_fields
        _assert_common_template_fields(operator.template_fields)

    def test_missing_required_params(self):
        with pytest.raises(AirflowException) as excinfo:
            CloudLoggingDeleteSinkOperator(
                task_id=TASK_ID,
                sink_name=None,
                project_id=None,
            ).execute(context={})
        assert "Required parameters are missing" in str(excinfo.value)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    @pytest.mark.parametrize(("sink_config", "update_mask"), update_test_cases, ids=update_test_ids)
    def test_update_sink_success(self, hook_mock, sink_config, update_mask):
        hook_instance = hook_mock.return_value

        hook_instance.get_sink.return_value = sink
        sink_ = LogSink(**sink_config)
        hook_instance.update_sink.return_value = sink_

        operator = CloudLoggingUpdateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
            sink_config=sink_config,
            update_mask=update_mask,
        )

        result = operator.execute(context=mock.MagicMock())

        hook_instance.get_sink.assert_called_once_with(sink_name=SINK_NAME, project_id=PROJECT_ID)
        hook_instance.update_sink.assert_called_once()
        assert result == LogSink.to_dict(sink_)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    @pytest.mark.parametrize(("sink_config", "update_mask"), update_test_cases, ids=update_test_ids)
    def test_update_sink_raises_not_found(self, hook_mock, sink_config, update_mask):
        hook_instance = hook_mock.return_value

        hook_instance.get_sink.side_effect = NotFound("not found")

        operator = CloudLoggingUpdateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            sink_config=sink_config,
            update_mask=update_mask,
            project_id=PROJECT_ID,
        )

        with pytest.raises(NotFound, match="not found"):
            operator.execute(context=mock.MagicMock())

        hook_instance.get_sink.assert_called_once()
        hook_instance.update_sink.assert_not_called()

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    @pytest.mark.parametrize(("sink_config", "update_mask"), update_test_cases, ids=update_test_ids)
    def test_update_sink_raises_generic_error(self, hook_mock, sink_config, update_mask):
        hook_instance = hook_mock.return_value
        hook_instance.get_sink.side_effect = GoogleAPICallError("something went wrong")

        operator = CloudLoggingUpdateSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            sink_config=sink_config,
            update_mask=update_mask,
            project_id=PROJECT_ID,
        )

        with pytest.raises(GoogleAPICallError, match="something went wrong"):
            operator.execute(context=mock.MagicMock())
        hook_instance.get_sink.assert_called_once()
        hook_instance.update_sink.assert_not_called()

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    @pytest.mark.parametrize(
        "impersonation_chain",
        [
            ["user1@project.iam.gserviceaccount.com", "user2@project.iam.gserviceaccount.com"],
            "user2@project.iam.gserviceaccount.com",
        ],
    )
    def test_create_with_impersonation_chain(self, hook_mock, impersonation_chain):
        hook_instance = hook_mock.return_value
        hook_instance.get_sink.return_value = sink
        hook_instance.update_sink.return_value = sink

        operator = CloudLoggingUpdateSinkOperator(
            task_id=TASK_ID,
            sink_config=update_test_cases[0][0],
            update_mask=update_test_cases[0][1],
            sink_name=SINK_NAME,
            impersonation_chain=impersonation_chain,
            project_id=PROJECT_ID,
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=impersonation_chain,
        )

    def test_missing_rendered_field_raises(self):
        with DAG(
            dag_id="test_render_native",
            start_date=datetime(1997, 9, 25),
            render_template_as_native_obj=True,
        ) as dag:
            operator = CloudLoggingUpdateSinkOperator(
                task_id=TASK_ID,
                sink_name="{{ var.value.sink_name }}",
                sink_config="{{ var.value.sink_config }}",
                update_mask="{{ var.value.update_mask }}",
                project_id="{{ var.value.project_id }}",
                dag=dag,
            )

        context = {
            "var": {
                "value": {
                    "project_id": PROJECT_ID,
                    "sink_name": None,
                    "sink_config": None,
                    "update_mask": None,
                }
            },
        }

        operator.render_template_fields(context)

        with pytest.raises(
            AirflowException,
            match=re.escape(
                "Required parameters are missing: ['sink_name', 'sink_config', 'update_mask']. These must be passed as keyword parameters."
            ),
        ):
            operator.execute(context)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    @pytest.mark.parametrize(("sink_config", "update_mask"), update_test_cases, ids=update_test_ids)
    def test_template_rendering(self, hook_mock, sink_config, update_mask):
        with DAG(
            dag_id="test_render_native",
            start_date=datetime(2024, 1, 1),
            render_template_as_native_obj=True,
        ) as dag:
            operator = CloudLoggingUpdateSinkOperator(
                task_id=TASK_ID,
                sink_name="{{ var.value.sink_name }}",
                update_mask="{{ var.value.update_mask }}",
                sink_config="{{ var.value.sink_config }}",
                project_id="{{ var.value.project_id }}",
                unique_writer_identity="{{ var.value.unique_writer_identity }}",
                dag=dag,
            )

        context = {
            "var": {
                "value": {
                    "project_id": PROJECT_ID,
                    "sink_config": sink_config,
                    "sink_name": SINK_NAME,
                    "update_mask": update_mask,
                    "unique_writer_identity": UNIQUE_WRITER_IDENTITY,
                }
            }
        }

        hook_instance = hook_mock.return_value
        hook_instance.get_sink.return_value = LogSink(name=SINK_NAME)
        hook_instance.update_sink.return_value = LogSink(**sink_config)

        operator.render_template_fields(context)

        result = operator.execute(context=mock.MagicMock())

        # Assertions
        assert isinstance(operator.sink_config, dict)
        assert isinstance(operator.update_mask, dict)
        assert isinstance(operator.unique_writer_identity, bool)
        assert operator.sink_config["name"] == sink_config["name"]
        assert result["name"] == sink_config["name"]
        assert operator.update_mask == update_mask

        hook_instance.update_sink.assert_called_once_with(
            project_id=PROJECT_ID,
            sink_name=SINK_NAME,
            sink=sink_config,
            update_mask=update_mask,
            unique_writer_identity=UNIQUE_WRITER_IDENTITY,
        )

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    @pytest.mark.parametrize(("sink_config", "update_mask"), update_test_cases, ids=update_test_ids)
    def test_template_rendering_with_proto(self, hook_mock, sink_config, update_mask):
        sink_obj = LogSink(**sink_config)
        mask_obj = FieldMask(paths=update_mask["paths"])
        with DAG(
            dag_id="test_render_native_proto",
            start_date=datetime(2024, 1, 1),
            render_template_as_native_obj=True,
        ) as dag:
            operator = CloudLoggingUpdateSinkOperator(
                task_id=TASK_ID,
                sink_name="{{ var.value.sink_name }}",
                update_mask="{{ var.value.update_mask }}",
                sink_config="{{ var.value.sink_config }}",
                project_id="{{ var.value.project_id }}",
                unique_writer_identity="{{ var.value.unique_writer_identity }}",
                dag=dag,
            )

        context = {
            "var": {
                "value": {
                    "project_id": PROJECT_ID,
                    "sink_name": SINK_NAME,
                    "sink_config": sink_obj,
                    "update_mask": mask_obj,
                    "unique_writer_identity": UNIQUE_WRITER_IDENTITY,
                }
            }
        }

        hook_instance = hook_mock.return_value
        hook_instance.get_sink.return_value = LogSink(name=SINK_NAME)
        hook_instance.update_sink.return_value = sink_obj

        operator.render_template_fields(context)

        result = operator.execute(context=mock.MagicMock())

        assert isinstance(operator.sink_config, LogSink)
        assert isinstance(operator.update_mask, FieldMask)
        assert isinstance(operator.unique_writer_identity, bool)
        assert operator.sink_config.name == sink_obj.name
        assert result["name"] == sink_obj.name
        assert operator.update_mask == mask_obj
        assert operator.sink_config == sink_obj

        hook_instance.update_sink.assert_called_once_with(
            project_id=PROJECT_ID,
            sink_name=SINK_NAME,
            sink=sink_obj,
            update_mask=mask_obj,
            unique_writer_identity=UNIQUE_WRITER_IDENTITY,
        )
