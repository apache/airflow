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
from mock import MagicMock

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
    UUID = '12345678'
    JOB_TO_SUBMIT = {
        'job':
            {'placement': {'clusterName': 'test-cluster-name'},
             'reference': {'jobId': '{}_{}'.format(TASK_ID, UUID)}}
    }

    def setUp(self):
        self.mock_dataproc = MagicMock()

    def test_do_not_resubmit_job_if_same_job_running_on_cluster(self):
        # If a job with the same task ID is already running on the cluster, don't resubmit the job.
        mock_job_on_cluster = {'reference': {'jobId': '{}_{}'.format(TASK_ID, self.UUID)},
                               'status': {'state': 'RUNNING'}}

        self.mock_dataproc.projects.return_value.regions.return_value.jobs.return_value.list.return_value. \
            execute.return_value = {'jobs': [mock_job_on_cluster]}

        _DataProcJob(dataproc_api=self.mock_dataproc,
                     project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
                     job=self.JOB_TO_SUBMIT)

        self.mock_dataproc.projects.return_value.regions.return_value.jobs.return_value.submit\
            .assert_not_called()

    def test_keep_looking_for_recoverable_job_even_if_errored_job_exists_on_cluster(self):
        # Keep looking for a job to reattach to even if the first matching job found is in an irrecoverable
        #  state
        mock_job_on_cluster_running = {'reference': {'jobId': '{}_{}'.format(TASK_ID, self.UUID)},
                                       'status': {'state': 'RUNNING'}}

        mock_job_on_cluster_error = {'reference': {'jobId': '{}_{}'.format(TASK_ID, self.UUID)},
                                     'status': {'state': 'ERROR'}}

        self.mock_dataproc.projects.return_value.regions.return_value.jobs.return_value.list.return_value\
            .execute.return_value = {'jobs': [mock_job_on_cluster_error, mock_job_on_cluster_running]}

        _DataProcJob(dataproc_api=self.mock_dataproc,
                     project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
                     job=self.JOB_TO_SUBMIT)

        self.mock_dataproc.projects.return_value.regions.return_value.jobs.return_value.submit\
            .assert_not_called()

    def test_submit_job_if_different_job_running_on_cluster(self):
        # If there are jobs running on the cluster, but none of them have the same task ID as the job we're
        #  about to submit, then submit the job.
        mock_job_on_cluster = {'reference': {'jobId': 'a-different-job-id_{}'.format(self.UUID)},
                               'status': {'state': 'RUNNING'}}

        self.mock_dataproc.projects.return_value.regions.return_value.jobs.return_value.list.return_value\
            .execute.return_value = {'jobs': [mock_job_on_cluster]}

        _DataProcJob(dataproc_api=self.mock_dataproc,
                     project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
                     job=self.JOB_TO_SUBMIT)

        self.mock_dataproc.projects.return_value.regions.return_value.jobs.return_value.submit\
            .assert_called_once_with(projectId=GCP_PROJECT_ID_HOOK_UNIT_TEST,
                                     region=GCP_REGION,
                                     body=self.JOB_TO_SUBMIT)

    def test_submit_job_if_no_jobs_running_on_cluster(self):
        # If there are no other jobs already running on the cluster, then submit the job.
        self.mock_dataproc.projects.return_value.regions.return_value.jobs.return_value.list.return_value\
            .execute.return_value = {'jobs': []}

        _DataProcJob(dataproc_api=self.mock_dataproc,
                     project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
                     job=self.JOB_TO_SUBMIT)

        self.mock_dataproc.projects.return_value.regions.return_value.jobs.return_value.submit\
            .assert_called_once_with(projectId=GCP_PROJECT_ID_HOOK_UNIT_TEST,
                                     region=GCP_REGION,
                                     body=self.JOB_TO_SUBMIT)

    def test_submit_job_if_same_job_errored_on_cluster(self):
        # If a job with the same task ID finished with error on the cluster, then resubmit the job for retry.
        mock_job_on_cluster = {'reference': {'jobId': '{}_{}'.format(TASK_ID, self.UUID)},
                               'status': {'state': 'ERROR'}}

        self.mock_dataproc.projects.return_value.regions.return_value.jobs.return_value.list.return_value\
            .execute.return_value = {'jobs': [mock_job_on_cluster]}

        _DataProcJob(dataproc_api=self.mock_dataproc,
                     project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
                     job=self.JOB_TO_SUBMIT)

        self.mock_dataproc.projects.return_value.regions.return_value.jobs.return_value.submit\
            .assert_called_once_with(projectId=GCP_PROJECT_ID_HOOK_UNIT_TEST,
                                     region=GCP_REGION,
                                     body=self.JOB_TO_SUBMIT)

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
