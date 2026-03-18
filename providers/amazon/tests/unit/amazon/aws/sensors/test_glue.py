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
from unittest.mock import ANY

import pytest

from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred


class TestGlueJobSensor:
    @mock.patch.object(GlueJobHook, "print_job_logs")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "get_job_state")
    def test_poke(self, mock_get_job_state, mock_conn, mock_print_job_logs):
        mock_conn.return_value.get_job_run()
        mock_get_job_state.return_value = "SUCCEEDED"
        op = GlueJobSensor(
            task_id="test_glue_job_sensor",
            job_name="aws_test_glue_job",
            run_id="5152fgsfsjhsh61661",
            poke_interval=1,
            timeout=5,
        )

        assert op.poke({})
        mock_print_job_logs.assert_not_called()

    @mock.patch.object(GlueJobHook, "print_job_logs")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "get_job_state")
    def test_poke_with_verbose_logging(self, mock_get_job_state, mock_conn, mock_print_job_logs):
        mock_conn.return_value.get_job_run()
        mock_get_job_state.return_value = "SUCCEEDED"
        job_name = "job_name"
        job_run_id = "job_run_id"
        op = GlueJobSensor(
            task_id="test_glue_job_sensor",
            job_name=job_name,
            run_id=job_run_id,
            poke_interval=1,
            timeout=5,
            verbose=True,
        )

        assert op.poke({})
        mock_print_job_logs.assert_called_once_with(
            job_name=job_name,
            run_id=job_run_id,
            continuation_tokens=ANY,
        )

    @mock.patch.object(GlueJobHook, "print_job_logs")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "get_job_state")
    def test_poke_false(self, mock_get_job_state, mock_conn, mock_print_job_logs):
        mock_conn.return_value.get_job_run()
        mock_get_job_state.return_value = "RUNNING"
        op = GlueJobSensor(
            task_id="test_glue_job_sensor",
            job_name="aws_test_glue_job",
            run_id="5152fgsfsjhsh61661",
            poke_interval=1,
            timeout=5,
        )

        assert not op.poke({})
        mock_print_job_logs.assert_not_called()

    @mock.patch.object(GlueJobHook, "print_job_logs")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "get_job_state")
    def test_poke_false_with_verbose_logging(self, mock_get_job_state, mock_conn, mock_print_job_logs):
        mock_conn.return_value.get_job_run()
        mock_get_job_state.return_value = "RUNNING"
        job_name = "job_name"
        job_run_id = "job_run_id"
        op = GlueJobSensor(
            task_id="test_glue_job_sensor",
            job_name=job_name,
            run_id=job_run_id,
            poke_interval=1,
            timeout=5,
            verbose=True,
        )

        assert not op.poke({})
        mock_print_job_logs.assert_called_once_with(
            job_name=job_name,
            run_id=job_run_id,
            continuation_tokens=ANY,
        )

    @mock.patch.object(GlueJobHook, "print_job_logs")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "get_job_state")
    def test_poke_failed_job_with_verbose_logging(self, mock_get_job_state, mock_conn, mock_print_job_logs):
        mock_conn.return_value.get_job_run()
        mock_get_job_state.return_value = "FAILED"
        job_name = "job_name"
        job_run_id = "job_run_id"
        op = GlueJobSensor(
            task_id="test_glue_job_sensor",
            job_name=job_name,
            run_id=job_run_id,
            poke_interval=1,
            timeout=5,
            verbose=True,
        )

        with pytest.raises(AirflowException):
            assert not op.poke({})
        mock_print_job_logs.assert_called_once_with(
            job_name=job_name,
            run_id=job_run_id,
            continuation_tokens=ANY,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.glue.GlueJobHook.get_job_state")
    def test_fail_poke(self, get_job_state):
        job_name = "job_name"
        job_run_id = "job_run_id"
        op = GlueJobSensor(
            task_id="test_glue_job_sensor",
            job_name=job_name,
            run_id=job_run_id,
            poke_interval=1,
            timeout=5,
            verbose=True,
        )
        op.verbose = False
        job_state = "FAILED"
        get_job_state.return_value = job_state
        job_error_message = "Exiting Job"
        with pytest.raises(AirflowException, match=job_error_message):
            op.poke(context={})

    def test_deferrable_execute_raises_task_deferred(self):
        job_name = "job_name"
        job_run_id = "job_run_id"
        sensor = GlueJobSensor(
            task_id="test_glue_job_sensor",
            job_name=job_name,
            run_id=job_run_id,
            deferrable=True,
            poke_interval=1,
            timeout=5,
        )
        with pytest.raises(TaskDeferred):
            sensor.execute({})

    @mock.patch.object(GlueJobSensor, "defer")
    def test_default_timeout(self, mock_defer):
        mock_defer.side_effect = TaskDeferred(trigger=mock.Mock(), method_name="execute_complete")
        sensor = GlueJobSensor(
            task_id="test_glue_job_sensor",
            job_name="job_name",
            run_id="job_run_id",
            deferrable=True,
            poke_interval=5,
            max_retries=30,
        )
        with pytest.raises(TaskDeferred):
            sensor.execute({})
        call_kwargs = mock_defer.call_args.kwargs["trigger"]
        assert call_kwargs.attempts == 30
        mock_defer.assert_called_once()

    def test_default_args(self):
        job_name = "job_name"
        job_run_id = "job_run_id"
        sensor = GlueJobSensor(
            task_id="test_glue_job_sensor",
            job_name=job_name,
            run_id=job_run_id,
        )
        assert sensor.poke_interval == 120
        assert sensor.verbose is False
        assert sensor.deferrable is False or isinstance(sensor.deferrable, bool)
        assert sensor.aws_conn_id == "aws_default"

    def test_custom_args(self):
        job_name = "job_name"
        job_run_id = "job_run_id"
        sensor = GlueJobSensor(
            task_id="test_glue_job_sensor",
            job_name=job_name,
            run_id=job_run_id,
            verbose=True,
            deferrable=True,
            poke_interval=10,
            aws_conn_id="custom_conn",
            max_retries=20,
        )
        assert sensor.verbose is True
        assert sensor.deferrable is True
        assert sensor.poke_interval == 10
        assert sensor.aws_conn_id == "custom_conn"
        assert sensor.max_retries == 20

    def test_defferable_params_passed_to_trigger(self):
        job_name = "job_name"
        job_run_id = "job_run_id"
        sensor = GlueJobSensor(
            task_id="test_glue_job_sensor",
            job_name=job_name,
            run_id=job_run_id,
            verbose=True,
            deferrable=True,
            poke_interval=10,
            region_name="us-west-2",
            aws_conn_id="custom_conn",
            max_retries=20,
        )
        with pytest.raises(TaskDeferred) as defer:
            sensor.execute({})

        assert defer.value.trigger.job_name == job_name
        assert defer.value.trigger.run_id == job_run_id
        assert defer.value.trigger.region_name == "us-west-2"
        assert defer.value.trigger.verbose
        assert defer.value.trigger.waiter_delay == 10
        assert defer.value.trigger.attempts == 20
        assert defer.value.trigger.aws_conn_id == "custom_conn"
