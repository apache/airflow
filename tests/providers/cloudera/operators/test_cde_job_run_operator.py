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

"""Tests related to the CDE Job operator"""

import unittest
from datetime import datetime
from unittest import mock
from unittest.mock import Mock, call

from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.providers.cloudera.hooks.cde_hook import CdeHook
from airflow.providers.cloudera.operators.cde_operator import CdeRunJobOperator
from tests.providers.cloudera.utils import _get_call_arguments

TEST_JOB_NAME = 'testjob'
TEST_JOB_RUN_ID = 10
TEST_TIMEOUT = 4
TEST_JOB_POLL_INTERVAL = 1
TEST_VARIABLES = {'var1': 'someval_{{ ds_nodash }}'}
TEST_OVERRIDES = {'spark': {'conf': {'myparam': 'val_{{ ds_nodash }}'}}}
TEST_CONTEXT = {
    'ds': '2020-11-25',
    'ds_nodash': '20201125',
    'ts': '2020-11-25T00:00:00+00:00',
    'ts_nodash': '20201125T000000',
    'run_id': 'runid',
}

TEST_HOST = 'vc1.cde-2.cdp-3.cloudera.site'
TEST_SCHEME = 'http'
TEST_PORT = 9090
TEST_AK = "access_key"
TEST_PK = "private_key"
TEST_CUSTOM_CA_CERTIFICATE = "/ca_cert/letsencrypt-stg-root-x1.pem"
TEST_EXTRA = (
    f'{{"access_key": "{TEST_AK}", "private_key": "{TEST_PK}",' f'"ca_cert": "{TEST_CUSTOM_CA_CERTIFICATE}"}}'
)

TEST_DEFAULT_CONNECTION_DICT = {
    'conn_id': CdeHook.DEFAULT_CONN_ID,
    'conn_type': 'http',
    'host': TEST_HOST,
    'port': TEST_PORT,
    'schema': TEST_SCHEME,
    'extra': TEST_EXTRA,
}

TEST_DEFAULT_CONNECTION = Connection(
    conn_id=CdeHook.DEFAULT_CONN_ID,
    conn_type='http',
    host=TEST_HOST,
    port=TEST_PORT,
    schema=TEST_SCHEME,
    extra=TEST_EXTRA,
)


@mock.patch.object(CdeHook, 'get_connection', return_value=TEST_DEFAULT_CONNECTION)
class CdeRunJobOperatorTest(unittest.TestCase):

    """Test cases for CDE operator"""

    def test_init(self, get_connection: Mock):
        """Test constructor"""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
        )
        get_connection.assert_called()
        self.assertEqual(cde_operator.job_name, TEST_JOB_NAME)
        self.assertDictEqual(cde_operator.variables, TEST_VARIABLES)
        self.assertDictEqual(cde_operator.overrides, TEST_OVERRIDES)
        self.assertEqual(cde_operator.connection_id, CdeRunJobOperator.DEFAULT_CONNECTION_ID)
        self.assertEqual(cde_operator.wait, CdeRunJobOperator.DEFAULT_WAIT)
        self.assertEqual(cde_operator.timeout, CdeRunJobOperator.DEFAULT_TIMEOUT)
        self.assertEqual(cde_operator.job_poll_interval, CdeRunJobOperator.DEFAULT_POLL_INTERVAL)
        self.assertEqual(cde_operator.api_retries, CdeRunJobOperator.DEFAULT_RETRIES)

    @mock.patch.object(CdeHook, 'submit_job', return_value=TEST_JOB_RUN_ID)
    @mock.patch.object(CdeHook, 'check_job_run_status', side_effect=['starting', 'running', 'succeeded'])
    def test_execute_and_wait(self, check_job_mock, submit_mock, get_connection):
        """Test executing a job run and waiting for success"""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        get_connection.assert_called()
        cde_operator.execute(TEST_CONTEXT)
        # Python 3.8 works with called_args = submit_mock.call_args.kwargs,
        # but kwargs method is missing in <=3.7.1
        called_args = _get_call_arguments(submit_mock.call_args)
        self.assertIsInstance(called_args, dict)
        self.assertEqual(dict(called_args['variables'], **TEST_VARIABLES), called_args['variables'])
        self.assertEqual(dict(called_args['variables'], **TEST_CONTEXT), called_args['variables'])
        self.assertDictEqual(TEST_OVERRIDES, called_args['overrides'])
        check_job_mock.assert_has_calls(
            [
                call(TEST_JOB_RUN_ID),
                call(TEST_JOB_RUN_ID),
                call(TEST_JOB_RUN_ID),
            ]
        )

    @mock.patch.object(CdeHook, 'submit_job', return_value=TEST_JOB_RUN_ID)
    @mock.patch.object(CdeHook, 'check_job_run_status')
    def test_execute_and_do_not_wait(self, check_job_mock, submit_mock, get_connection):
        """Test executing a job and not waiting"""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
            wait=False,
        )
        get_connection.assert_called()
        cde_operator.execute(TEST_CONTEXT)
        # Python 3.8 works with called_args = submit_mock.call_args.kwargs,
        # but kwargs method is missing in <=3.7.1
        called_args = _get_call_arguments(submit_mock.call_args)
        self.assertEqual(dict(called_args['variables'], **TEST_VARIABLES), called_args['variables'])
        self.assertEqual(dict(called_args['variables'], **TEST_CONTEXT), called_args['variables'])
        self.assertDictEqual(TEST_OVERRIDES, called_args['overrides'])
        check_job_mock.assert_not_called()

    @mock.patch.object(CdeHook, 'kill_job_run')
    def test_on_kill(self, kill_job_mock, get_connection):
        """Test killing a running job"""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        get_connection.assert_called()
        cde_operator._job_run_id = 1  # pylint: disable=W0212
        cde_operator.on_kill()
        kill_job_mock.assert_called()

    @mock.patch.object(CdeHook, 'check_job_run_status', return_value='starting')
    def test_wait_for_job_times_out(self, check_job_mock, get_connection):
        """Test a job run timeout"""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        get_connection.assert_called()
        try:
            cde_operator.wait_for_job()
        except TimeoutError:
            self.assertRaisesRegex(TimeoutError, f'Job run did not complete in {TEST_TIMEOUT} seconds')
            check_job_mock.assert_called()

    @mock.patch.object(CdeHook, 'check_job_run_status', side_effect=['failed', 'killed', 'unknown'])
    def test_wait_for_job_fails_failed_status(self, check_job_mock, get_connection):
        """Test a failed job run"""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        get_connection.assert_called()
        for status in ['failed', 'killed', 'unknown']:
            try:
                cde_operator.wait_for_job()
            except AirflowException:
                self.assertRaisesRegex(AirflowException, f'Job run exited with {status} status')
                check_job_mock.assert_called()

    @mock.patch.object(CdeHook, 'check_job_run_status', return_value='not_a_status')
    def test_wait_for_job_fails_unexpected_status(self, check_job_mock, get_connection):
        """Test an unusual status from API"""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        get_connection.assert_called()
        try:
            cde_operator.wait_for_job()
        except AirflowException:
            self.assertRaisesRegex(
                AirflowException, 'Got unexpected status when polling for job: not_a_status'
            )
            check_job_mock.assert_called()

    def test_templating(self, get_connection):
        """Test templated fields"""
        dag = DAG("dagid", start_date=datetime.now())
        cde_operator = CdeRunJobOperator(
            dag=dag,
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
        )
        get_connection.assert_called()
        cde_operator.render_template_fields(TEST_CONTEXT)
        self.assertEqual(dict(cde_operator.variables, **{'var1': 'someval_20201125'}), cde_operator.variables)
        self.assertDictEqual(cde_operator.overrides, {'spark': {'conf': {'myparam': 'val_20201125'}}})


if __name__ == "__main__":
    unittest.main()
