# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import sys
import time
import unittest
from datetime import datetime, timedelta
from mock import patch

from airflow import DAG, configuration, settings
from airflow.exceptions import (AirflowException,
                                AirflowSensorTimeout,
                                AirflowSkipException)
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import HttpSensor, BaseSensorOperator, HdfsSensor, ExternalTaskSensor
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

configuration.load_test_config()

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'


class TimeoutTestSensor(BaseSensorOperator):
    """
    Sensor that always returns the return_value provided

    :param return_value: Set to true to mark the task as SKIPPED on failure
    :type return_value: any
    """

    @apply_defaults
    def __init__(
            self,
            return_value=False,
            *args,
            **kwargs):
        self.return_value = return_value
        super(TimeoutTestSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        return self.return_value

    def execute(self, context):
        started_at = datetime.now()
        time_jump = self.params.get('time_jump')
        while not self.poke(context):
            if time_jump:
                started_at -= time_jump
            if (datetime.now() - started_at).total_seconds() > self.timeout:
                if self.soft_fail:
                    raise AirflowSkipException('Snap. Time is OUT.')
                else:
                    raise AirflowSensorTimeout('Snap. Time is OUT.')
            time.sleep(self.poke_interval)
        self.logger.info("Success criteria met. Exiting.")


class SensorTimeoutTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_timeout(self):
        t = TimeoutTestSensor(
            task_id='test_timeout',
            execution_timeout=timedelta(days=2),
            return_value=False,
            poke_interval=5,
            params={'time_jump': timedelta(days=2, seconds=1)},
            dag=self.dag
            )
        self.assertRaises(
            AirflowSensorTimeout,
            t.run,
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)


class HttpSensorTests(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_poke_exception(self):
        """
        Exception occurs in poke function should not be ignored.
        """
        def resp_check(resp):
            raise AirflowException('AirflowException raised here!')

        task = HttpSensor(
            task_id='http_sensor_poke_exception',
            http_conn_id='http_default',
            endpoint='',
            request_params={},
            response_check=resp_check,
            poke_interval=5)
        with self.assertRaisesRegexp(AirflowException, 'AirflowException raised here!'):
            task.execute(None)

    @patch("airflow.hooks.http_hook.requests.Session.send")
    def test_head_method(self, mock_session_send):
        def resp_check(resp):
            return True

        task = HttpSensor(
            dag=self.dag,
            task_id='http_sensor_head_method',
            http_conn_id='http_default',
            endpoint='',
            request_params={},
            method='HEAD',
            response_check=resp_check,
            timeout=5,
            poke_interval=1)

        import requests
        task.execute(None)

        args, kwargs = mock_session_send.call_args
        received_request = args[0]

        prep_request = requests.Request(
            'HEAD',
            'https://www.google.com',
            {}).prepare()

        self.assertEqual(prep_request.url, received_request.url)
        self.assertTrue(prep_request.method, received_request.method)

    @patch("airflow.hooks.http_hook.requests.Session.send")
    def test_logging_head_error_request(
        self,
        mock_session_send
    ):

        def resp_check(resp):
            return True

        import requests
        response = requests.Response()
        response.status_code = 404
        response.reason = 'Not Found'
        mock_session_send.return_value = response

        task = HttpSensor(
            dag=self.dag,
            task_id='http_sensor_head_method',
            http_conn_id='http_default',
            endpoint='',
            request_params={},
            method='HEAD',
            response_check=resp_check,
            timeout=5,
            poke_interval=1
        )

        with mock.patch.object(task.hook.logger, 'error') as mock_errors:
            with self.assertRaises(AirflowSensorTimeout):
                task.execute(None)

            self.assertTrue(mock_errors.called)
            mock_errors.assert_called_with('HTTP error: %s', 'Not Found')


class HdfsSensorTests(unittest.TestCase):

    def setUp(self):
        if sys.version_info[0] == 3:
            raise unittest.SkipTest('HdfsSensor won\'t work with python3. No need to test anything here')
        from tests.core import FakeHDFSHook
        self.hook = FakeHDFSHook

    def test_legacy_file_exist(self):
        """
        Test the legacy behaviour
        :return:
        """
        # Given
        logging.info("Test for existing file with the legacy behaviour")
        # When
        task = HdfsSensor(task_id='Should_be_file_legacy',
                          filepath='/datadirectory/datafile',
                          timeout=1,
                          retry_delay=timedelta(seconds=1),
                          poke_interval=1,
                          hook=self.hook)
        task.execute(None)

        # Then
        # Nothing happens, nothing is raised exec is ok

    def test_legacy_file_exist_but_filesize(self):
        """
        Test the legacy behaviour with the filesize
        :return:
        """
        # Given
        logging.info("Test for existing file with the legacy behaviour")
        # When
        task = HdfsSensor(task_id='Should_be_file_legacy',
                          filepath='/datadirectory/datafile',
                          timeout=1,
                          file_size=20,
                          retry_delay=timedelta(seconds=1),
                          poke_interval=1,
                          hook=self.hook)

        # When
        # Then
        with self.assertRaises(AirflowSensorTimeout):
            task.execute(None)

    def test_legacy_file_does_not_exists(self):
        """
        Test the legacy behaviour
        :return:
        """
        # Given
        logging.info("Test for non existing file with the legacy behaviour")
        task = HdfsSensor(task_id='Should_not_be_file_legacy',
                          filepath='/datadirectory/not_existing_file_or_directory',
                          timeout=1,
                          retry_delay=timedelta(seconds=1),
                          poke_interval=1,
                          hook=self.hook)

        # When
        # Then
        with self.assertRaises(AirflowSensorTimeout):
            task.execute(None)


class ExternalTaskSensorTests(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        self.args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
            'depends_on_past': False}

    def test_external_task_sensor_fn_multiple_execution_dates(self):
        bash_command_code = """
{% set s=execution_date.time().second %}
echo "second is {{ s }}"
if [[ $(( {{ s }} % 60 )) == 1 ]]
    then
        exit 1
fi
exit 0
"""
        dag_external_id = TEST_DAG_ID + '_external'
        dag_external = DAG(
            dag_external_id,
            default_args=self.args,
            schedule_interval=timedelta(seconds=1))
        task_external_with_failure = BashOperator(
            task_id="task_external_with_failure",
            bash_command=bash_command_code,
            retries=0,
            dag=dag_external)
        task_external_without_failure = DummyOperator(
            task_id="task_external_without_failure",
            retries=0,
            dag=dag_external)

        task_external_without_failure.run(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + timedelta(seconds=1),
            ignore_ti_state=True)

        session = settings.Session()
        TI = TaskInstance
        try:
            task_external_with_failure.run(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE + timedelta(seconds=1),
                ignore_ti_state=True)
            # The test_with_failure task is excepted to fail
            # once per minute (the run on the first second of
            # each minute).
        except Exception as e:
            failed_tis = session.query(TI).filter(
                TI.dag_id == dag_external_id,
                TI.state == State.FAILED,
                TI.execution_date == DEFAULT_DATE + timedelta(seconds=1)).all()
            if (len(failed_tis) == 1 and
                    failed_tis[0].task_id == 'task_external_with_failure'):
                pass
            else:
                raise e

        dag_id = TEST_DAG_ID
        dag = DAG(
            dag_id,
            default_args=self.args,
            schedule_interval=timedelta(minutes=1))
        task_without_failure = ExternalTaskSensor(
            task_id='task_without_failure',
            external_dag_id=dag_external_id,
            external_task_id='task_external_without_failure',
            execution_date_fn=lambda dt: [dt + timedelta(seconds=i)
                                          for i in range(2)],
            allowed_states=['success'],
            retries=0,
            timeout=1,
            poke_interval=1,
            dag=dag)
        task_with_failure = ExternalTaskSensor(
            task_id='task_with_failure',
            external_dag_id=dag_external_id,
            external_task_id='task_external_with_failure',
            execution_date_fn=lambda dt: [dt + timedelta(seconds=i)
                                          for i in range(2)],
            allowed_states=['success'],
            retries=0,
            timeout=1,
            poke_interval=1,
            dag=dag)

        task_without_failure.run(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_ti_state=True)

        with self.assertRaises(AirflowSensorTimeout):
            task_with_failure.run(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE,
                ignore_ti_state=True)
