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

import unittest
from unittest import mock
from unittest.mock import ANY

import pytest

from airflow import AirflowException
from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor


class TestGlueJobSensor(unittest.TestCase):
    def setUp(self):
        conf.load_test_config()

    @mock.patch.object(GlueJobHook, 'print_job_logs')
    @mock.patch.object(GlueJobHook, 'get_conn')
    @mock.patch.object(GlueJobHook, 'get_job_state')
    def test_poke(self, mock_get_job_state, mock_conn, mock_print_job_logs):
        mock_conn.return_value.get_job_run()
        mock_get_job_state.return_value = 'SUCCEEDED'
        op = GlueJobSensor(
            task_id='test_glue_job_sensor',
            job_name='aws_test_glue_job',
            run_id='5152fgsfsjhsh61661',
            poke_interval=1,
            timeout=5,
        )

        assert op.poke({})
        mock_print_job_logs.assert_not_called()

    @mock.patch.object(GlueJobHook, 'print_job_logs')
    @mock.patch.object(GlueJobHook, 'get_conn')
    @mock.patch.object(GlueJobHook, 'get_job_state')
    def test_poke_with_verbose_logging(self, mock_get_job_state, mock_conn, mock_print_job_logs):
        mock_conn.return_value.get_job_run()
        mock_get_job_state.return_value = 'SUCCEEDED'
        job_name = 'job_name'
        job_run_id = 'job_run_id'
        op = GlueJobSensor(
            task_id='test_glue_job_sensor',
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
            job_failed=False,
            next_token=ANY,
        )

    @mock.patch.object(GlueJobHook, 'print_job_logs')
    @mock.patch.object(GlueJobHook, 'get_conn')
    @mock.patch.object(GlueJobHook, 'get_job_state')
    def test_poke_false(self, mock_get_job_state, mock_conn, mock_print_job_logs):
        mock_conn.return_value.get_job_run()
        mock_get_job_state.return_value = 'RUNNING'
        op = GlueJobSensor(
            task_id='test_glue_job_sensor',
            job_name='aws_test_glue_job',
            run_id='5152fgsfsjhsh61661',
            poke_interval=1,
            timeout=5,
        )

        assert not op.poke({})
        mock_print_job_logs.assert_not_called()

    @mock.patch.object(GlueJobHook, 'print_job_logs')
    @mock.patch.object(GlueJobHook, 'get_conn')
    @mock.patch.object(GlueJobHook, 'get_job_state')
    def test_poke_false_with_verbose_logging(self, mock_get_job_state, mock_conn, mock_print_job_logs):
        mock_conn.return_value.get_job_run()
        mock_get_job_state.return_value = 'RUNNING'
        job_name = 'job_name'
        job_run_id = 'job_run_id'
        op = GlueJobSensor(
            task_id='test_glue_job_sensor',
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
            job_failed=False,
            next_token=ANY,
        )

    @mock.patch.object(GlueJobHook, 'print_job_logs')
    @mock.patch.object(GlueJobHook, 'get_conn')
    @mock.patch.object(GlueJobHook, 'get_job_state')
    def test_poke_failed_job_with_verbose_logging(self, mock_get_job_state, mock_conn, mock_print_job_logs):
        mock_conn.return_value.get_job_run()
        mock_get_job_state.return_value = 'FAILED'
        job_name = 'job_name'
        job_run_id = 'job_run_id'
        op = GlueJobSensor(
            task_id='test_glue_job_sensor',
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
                log_group_suffix='error',
                filter_pattern='?ERROR ?Exception',
                next_token=ANY,
            )


if __name__ == '__main__':
    unittest.main()
