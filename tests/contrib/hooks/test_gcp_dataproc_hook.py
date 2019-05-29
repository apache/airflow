# -*- coding: utf-8 -*-
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
#

import unittest
from airflow.contrib.hooks.gcp_dataproc_hook import _DataProcJob
from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
from tests.contrib.utils.base_gcp_mock import GCP_PROJECT_ID_HOOK_UNIT_TEST
from tests.compat import mock

JOB = 'test-job'
GCP_REGION = 'global'
TASK_ID = 'test-task-id'

BASE_STRING = 'airflow.contrib.hooks.gcp_api_base_hook.{}'
DATAPROC_STRING = 'airflow.contrib.hooks.gcp_dataproc_hook.{}'


def mock_init(self, gcp_conn_id, delegate_to=None):
    pass


class DataProcHookTest(unittest.TestCase):
    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleCloudBaseHook.__init__'),
                        new=mock_init):
            self.dataproc_hook = DataProcHook()

    @mock.patch(DATAPROC_STRING.format('_DataProcJob'))
    def test_submit(self, job_mock):
        with mock.patch(DATAPROC_STRING.format('DataProcHook.get_conn',
                                               return_value=None)):
            self.dataproc_hook.submit(GCP_PROJECT_ID_HOOK_UNIT_TEST, JOB)
            job_mock.assert_called_once_with(mock.ANY, GCP_PROJECT_ID_HOOK_UNIT_TEST, JOB, GCP_REGION,
                                             job_error_states=mock.ANY, num_retries=mock.ANY)


class DataProcJobTest(unittest.TestCase):
    @mock.patch(DATAPROC_STRING.format('_DataProcJob.__init__'), return_value=None)
    def test_raise_error_default_job_error_states(self, mock_init):
        job = _DataProcJob()
        job.job = {'status': {'state': 'ERROR'}}
        job.job_error_states = None
        with self.assertRaises(Exception) as cm:
            job.raise_error()
        self.assertIn('ERROR', str(cm.exception))

    @mock.patch(DATAPROC_STRING.format('_DataProcJob.__init__'), return_value=None)
    def test_raise_error_custom_job_error_states(self, mock_init):
        job = _DataProcJob()
        job.job = {'status': {'state': 'CANCELLED'}}
        job.job_error_states = ['ERROR', 'CANCELLED']
        with self.assertRaises(Exception) as cm:
            job.raise_error()
        self.assertIn('CANCELLED', str(cm.exception))

    @mock.patch(DATAPROC_STRING.format('_DataProcJob.__init__'), return_value=None)
    def test_raise_error_fallback_job_error_states(self, mock_init):
        job = _DataProcJob()
        job.job = {'status': {'state': 'ERROR'}}
        job.job_error_states = ['CANCELLED']
        with self.assertRaises(Exception) as cm:
            job.raise_error()
        self.assertIn('ERROR', str(cm.exception))

    @mock.patch(DATAPROC_STRING.format('_DataProcJob.__init__'), return_value=None)
    def test_raise_error_with_state_done(self, mock_init):
        job = _DataProcJob()
        job.job = {'status': {'state': 'DONE'}}
        job.job_error_states = None
        try:
            job.raise_error()
            # Pass test
        except Exception:
            self.fail("raise_error() should not raise Exception when job=%s" % job.job)
