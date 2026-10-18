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

import json
from datetime import datetime, timedelta
from os.path import dirname
from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models.trigger import Trigger
from airflow.providers.common.compat.sdk import DAG, Context, TaskDeferred, timezone
from airflow.providers.microsoft.azure.operators.msgraph import MSGraphAsyncOperator
from airflow.providers.microsoft.azure.sensors.msgraph import MSGraphSensor
from airflow.providers.microsoft.azure.triggers.msgraph import MSGraphTrigger
from airflow.triggers.base import TriggerEvent
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.file_loading import load_json_from_resources
from tests_common.test_utils.operators.run_deferrable import execute_operator
from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS
from unit.microsoft.azure.test_utils import mock_json_response, patch_hook_and_request_adapter


class TestMSGraphSensor:
    def test_execute_with_result_processor_with_old_signature(self):
        status = load_json_from_resources(dirname(__file__), "..", "resources", "status.json")
        response = mock_json_response(200, *status)

        with patch_hook_and_request_adapter(response):
            sensor = MSGraphSensor(
                task_id="check_workspaces_status",
                conn_id="powerbi",
                url="myorg/admin/workspaces/scanStatus/{scanId}",
                path_parameters={"scanId": "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"},
                result_processor=lambda context, result: result["id"],
                retry_delay=5,
                timeout=5,
            )

            with pytest.warns(
                AirflowProviderDeprecationWarning,
                match="result_processor signature has changed, result parameter should be defined before context!",
            ):
                results, events = execute_operator(sensor)

            assert sensor.path_parameters == {"scanId": "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"}
            assert isinstance(results, str)
            assert results == "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"
            assert len(events) == 3
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["status"] == "success"
            assert events[0].payload["type"] == "builtins.dict"
            assert events[0].payload["response"] == json.dumps(status[0])
            assert isinstance(events[1], TriggerEvent)
            assert isinstance(events[1].payload, datetime)
            assert isinstance(events[2], TriggerEvent)
            assert events[2].payload["status"] == "success"
            assert events[2].payload["type"] == "builtins.dict"
            assert events[2].payload["response"] == json.dumps(status[1])

    def test_execute_with_result_processor_with_new_signature(self):
        status = load_json_from_resources(dirname(__file__), "..", "resources", "status.json")
        response = mock_json_response(200, *status)

        with patch_hook_and_request_adapter(response):
            sensor = MSGraphSensor(
                task_id="check_workspaces_status",
                conn_id="powerbi",
                url="myorg/admin/workspaces/scanStatus/{scanId}",
                path_parameters={"scanId": "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"},
                result_processor=lambda result, **context: result["id"],
                retry_delay=5,
                timeout=5,
            )

            results, events = execute_operator(sensor)

            assert sensor.path_parameters == {"scanId": "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"}
            assert isinstance(results, str)
            assert results == "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"
            assert len(events) == 3
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["status"] == "success"
            assert events[0].payload["type"] == "builtins.dict"
            assert events[0].payload["response"] == json.dumps(status[0])
            assert isinstance(events[1], TriggerEvent)
            assert isinstance(events[1].payload, datetime)
            assert isinstance(events[2], TriggerEvent)
            assert events[2].payload["status"] == "success"
            assert events[2].payload["type"] == "builtins.dict"
            assert events[2].payload["response"] == json.dumps(status[1])

    def test_execute_with_lambda_parameter_and_result_processor_with_new_signature(self):
        status = load_json_from_resources(dirname(__file__), "..", "resources", "status.json")
        response = mock_json_response(200, *status)

        with patch_hook_and_request_adapter(response):
            sensor = MSGraphSensor(
                task_id="check_workspaces_status",
                conn_id="powerbi",
                url="myorg/admin/workspaces/scanStatus/{scanId}",
                path_parameters=lambda context, jinja_env: {"scanId": "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"},
                result_processor=lambda result, **context: result["id"],
                retry_delay=5,
                timeout=5,
            )

            results, events = execute_operator(sensor)

            assert sensor.path_parameters == {"scanId": "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"}
            assert isinstance(results, str)
            assert results == "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"
            assert len(events) == 3
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["status"] == "success"
            assert events[0].payload["type"] == "builtins.dict"
            assert events[0].payload["response"] == json.dumps(status[0])
            assert isinstance(events[1], TriggerEvent)
            assert isinstance(events[1].payload, datetime)
            assert isinstance(events[2], TriggerEvent)
            assert events[2].payload["status"] == "success"
            assert events[2].payload["type"] == "builtins.dict"
            assert events[2].payload["response"] == json.dumps(status[1])

    def test_template_fields(self):
        sensor = MSGraphSensor(
            task_id="check_workspaces_status",
            conn_id="powerbi",
            url="myorg/admin/workspaces/scanStatus/{scanId}",
        )

        for template_field in MSGraphSensor.template_fields:
            getattr(sensor, template_field)

    def test_execute_complete_passes_timeout_to_defer(self):
        sensor = MSGraphSensor(
            task_id="check_timeout",
            conn_id="powerbi",
            url="myorg/admin/workspaces/scanStatus/{scanId}",
            timeout=10,
        )

        with patch.object(sensor, "defer") as mock_defer:
            sensor.execute_complete(
                context={}, event={"status": "success", "response": json.dumps({"status": "running"})}
            )
            mock_defer.assert_called_once()
            assert mock_defer.call_args.kwargs["timeout"] == timedelta(seconds=10)


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="The triggerer renders templated fields from Airflow 3.3")
class TestMSGraphSensorStartFromTrigger:
    def test_start_from_trigger_builds_the_trigger_arguments(self):
        sensor = MSGraphSensor(
            task_id="check_workspaces_status",
            conn_id="powerbi",
            url="myorg/admin/workspaces/scanStatus/{scanId}",
            path_parameters={"scanId": "{{ params.scan_id }}"},
            timeout=10,
            start_from_trigger=True,
        )

        assert sensor.start_from_trigger is True
        assert sensor.start_trigger_args.trigger_cls == (
            "airflow.providers.microsoft.azure.triggers.msgraph.MSGraphTrigger"
        )
        assert sensor.start_trigger_args.next_method == "execute_complete"
        assert sensor.start_trigger_args.timeout == timedelta(seconds=10)
        assert sensor.start_trigger_args.trigger_kwargs == {
            "url": "myorg/admin/workspaces/scanStatus/{scanId}",
            "response_type": None,
            "path_parameters": {"scanId": "{{ params.scan_id }}"},
            "url_template": None,
            "method": "GET",
            "query_parameters": None,
            "headers": None,
            "data": None,
            "conn_id": "powerbi",
            "timeout": 10,
            "proxies": None,
            "scopes": None,
            "api_version": None,
            "serializer": "airflow.providers.microsoft.azure.triggers.msgraph.ResponseSerializer",
        }

    def test_start_from_trigger_defers_the_same_trigger_as_execute(self):
        sensor = MSGraphSensor(
            task_id="check_workspaces_status",
            conn_id="powerbi",
            url="myorg/admin/workspaces/scanStatus/{scanId}",
            path_parameters={"scanId": "{{ params.scan_id }}"},
            timeout=10,
            start_from_trigger=True,
        )

        with pytest.raises(TaskDeferred) as deferred:
            sensor.execute(context=Context())

        trigger = MSGraphTrigger(**sensor.start_trigger_args.trigger_kwargs)

        assert trigger.serialize() == deferred.value.trigger.serialize()
        assert sensor.start_trigger_args.next_method == deferred.value.method_name

    def test_start_from_trigger_falls_back_to_the_worker(self):
        with DAG(dag_id="msgraph_start_from_trigger", schedule=None):
            scan = MSGraphAsyncOperator(task_id="scan", conn_id="powerbi", url="myorg/admin/workspaces")
            sensor = MSGraphSensor(
                task_id="check_workspaces_status",
                conn_id="powerbi",
                url="myorg/admin/workspaces/scanStatus/{scanId}",
                path_parameters={"scanId": scan.output},
                start_from_trigger=True,
            )

        assert sensor.start_from_trigger is False
        assert sensor.start_trigger_args is None

    def test_start_trigger_args_are_not_shared_between_tasks(self):
        first = MSGraphSensor(
            task_id="first", conn_id="powerbi", url="first", timeout=10, start_from_trigger=True
        )
        second = MSGraphSensor(
            task_id="second", conn_id="powerbi", url="second", timeout=20, start_from_trigger=True
        )
        third = MSGraphSensor(task_id="third", conn_id="powerbi", url="third")

        assert first.start_trigger_args is not second.start_trigger_args
        assert first.start_trigger_args.trigger_kwargs["url"] == "first"
        assert first.start_trigger_args.timeout == timedelta(seconds=10)
        assert second.start_trigger_args.trigger_kwargs["url"] == "second"
        assert second.start_trigger_args.timeout == timedelta(seconds=20)
        assert third.start_from_trigger is False
        assert third.start_trigger_args is None
        assert MSGraphSensor.start_from_trigger is False
        assert MSGraphSensor.start_trigger_args is None

    def test_mapped_task_has_no_start_trigger_args(self):
        with DAG(dag_id="msgraph_start_from_trigger", schedule=None):
            mapped = MSGraphSensor.partial(
                task_id="check_workspaces_status",
                conn_id="powerbi",
                url="myorg/admin/workspaces/scanStatus/{scanId}",
                start_from_trigger=True,
            ).expand(path_parameters=[{"scanId": "first"}, {"scanId": "second"}])

        # The scheduler cannot expand the arguments of a mapped task, and without them it leaves
        # the task to a worker.
        assert mapped.start_trigger_args is None

    @pytest.mark.db_test
    @pytest.mark.need_serialized_dag
    def test_scheduler_defers_the_task_with_the_sensor_timeout(self, dag_maker, session, time_machine):
        now = timezone.datetime(2026, 10, 5, 12)
        time_machine.move_to(now, tick=False)

        with dag_maker(session=session):
            sensor = MSGraphSensor(
                task_id="check_workspaces_status",
                conn_id="powerbi",
                url="myorg/admin/workspaces/scanStatus/{scanId}",
                path_parameters={"scanId": "{{ params.scan_id }}"},
                timeout=10,
                start_from_trigger=True,
            )

        dag_run = dag_maker.create_dagrun()
        task_instance = dag_run.get_task_instance("check_workspaces_status", session=session)
        task_instance.task = dag_run.dag.get_task("check_workspaces_status")

        dag_run.schedule_tis((task_instance,), session=session)

        assert task_instance.state == TaskInstanceState.DEFERRED
        assert task_instance.next_method == "execute_complete"
        assert task_instance.trigger_timeout == now + timedelta(seconds=10)
        trigger = session.get(Trigger, task_instance.trigger_id)
        assert trigger.classpath == "airflow.providers.microsoft.azure.triggers.msgraph.MSGraphTrigger"
        assert trigger.kwargs == sensor.start_trigger_args.trigger_kwargs

    def test_execute_when_started_from_the_trigger(self):
        status = load_json_from_resources(dirname(__file__), "..", "resources", "status.json")
        response = mock_json_response(200, *status)

        with (
            patch_hook_and_request_adapter(response) as (*_, mock_get_http_response),
            patch.object(
                MSGraphSensor, "execute", autospec=True, side_effect=MSGraphSensor.execute
            ) as mock_execute,
        ):
            sensor = MSGraphSensor(
                task_id="check_workspaces_status",
                conn_id="powerbi",
                url="myorg/admin/workspaces/scanStatus/{scanId}",
                path_parameters={"scanId": "{{ ti.task_id }}"},
                result_processor=lambda result, **context: result["id"],
                retry_delay=1,
                timeout=5,
                start_from_trigger=True,
            )

            results, events = execute_operator(sensor)

        # The first poll comes from the trigger the scheduler created, with the path parameters it
        # rendered itself, so execute only runs for the poll which follows the retry delay.
        mock_execute.assert_called_once()
        assert [call.args[0].url for call in mock_get_http_response.call_args_list] == [
            "myorg/admin/workspaces/scanStatus/check_workspaces_status",
            "myorg/admin/workspaces/scanStatus/check_workspaces_status",
        ]
        assert results == "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"
        assert len(events) == 3
        assert events[0].payload["response"] == json.dumps(status[0])
        assert isinstance(events[1].payload, datetime)
        assert events[2].payload["response"] == json.dumps(status[1])
