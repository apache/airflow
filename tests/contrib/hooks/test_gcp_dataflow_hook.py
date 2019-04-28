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

import copy
import unittest

from airflow.contrib.hooks.gcp_dataflow_hook import (DataFlowHook, _Dataflow,
                                                     _DataflowJob)
from tests.compat import MagicMock, mock

TASK_ID = 'test-dataflow-operator'
JOB_NAME = 'test-dataflow-pipeline'
TEMPLATE = 'gs://dataflow-templates/wordcount/template_file'
PARAMETERS = {
    'inputFile': 'gs://dataflow-samples/shakespeare/kinglear.txt',
    'output': 'gs://test/output/my_output'
}
PY_FILE = 'apache_beam.examples.wordcount'
JAR_FILE = 'unitest.jar'
JOB_CLASS = 'com.example.UnitTest'
PY_OPTIONS = ['-m']
DATAFLOW_OPTIONS_PY = {
    'project': 'test',
    'staging_location': 'gs://test/staging',
    'labels': {'foo': 'bar'}
}
DATAFLOW_OPTIONS_JAVA = {
    'project': 'test',
    'stagingLocation': 'gs://test/staging',
    'labels': {'foo': 'bar'}
}
DATAFLOW_OPTIONS_TEMPLATE = {
    'project': 'test',
    'tempLocation': 'gs://test/temp',
    'zone': 'us-central1-f'
}
RUNTIME_ENV = {
    'tempLocation': 'gs://test/temp',
    'zone': 'us-central1-f',
    'numWorkers': 2,
    'maxWorkers': 10,
    'serviceAccountEmail': 'test@apache.airflow',
    'machineType': 'n1-standard-1',
    'additionalExperiments': ['exp_flag1', 'exp_flag2'],
    'network': 'default',
    'subnetwork': 'regions/REGION/subnetworks/SUBNETWORK',
    'additionalUserLabels': {
        'name': 'wrench',
        'mass': '1.3kg',
        'count': '3'
    }
}
BASE_STRING = 'airflow.contrib.hooks.gcp_api_base_hook.{}'
DATAFLOW_STRING = 'airflow.contrib.hooks.gcp_dataflow_hook.{}'
MOCK_UUID = '12345678'
TEST_PROJECT = 'test-project'
TEST_JOB_NAME = 'test-job-name'
TEST_JOB_ID = 'test-job-id'
TEST_LOCATION = 'us-central1'


def mock_init(self, gcp_conn_id, delegate_to=None):
    pass


class DataFlowHookTest(unittest.TestCase):

    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleCloudBaseHook.__init__'),
                        new=mock_init):
            self.dataflow_hook = DataFlowHook(gcp_conn_id='test')

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJob'))
    @mock.patch(DATAFLOW_STRING.format('_Dataflow'))
    @mock.patch(DATAFLOW_STRING.format('DataFlowHook.get_conn'))
    def test_start_python_dataflow(self, mock_conn,
                                   mock_dataflow, mock_dataflowjob, mock_uuid):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        self.dataflow_hook.start_python_dataflow(
            job_name=JOB_NAME, variables=DATAFLOW_OPTIONS_PY,
            dataflow=PY_FILE, py_options=PY_OPTIONS)
        EXPECTED_CMD = ['python2', '-m', PY_FILE,
                        '--region=us-central1',
                        '--runner=DataflowRunner', '--project=test',
                        '--labels=foo=bar',
                        '--staging_location=gs://test/staging',
                        '--job_name={}-{}'.format(JOB_NAME, MOCK_UUID)]
        self.assertListEqual(sorted(mock_dataflow.call_args[0][0]),
                             sorted(EXPECTED_CMD))

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJob'))
    @mock.patch(DATAFLOW_STRING.format('_Dataflow'))
    @mock.patch(DATAFLOW_STRING.format('DataFlowHook.get_conn'))
    def test_start_java_dataflow(self, mock_conn,
                                 mock_dataflow, mock_dataflowjob, mock_uuid):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        self.dataflow_hook.start_java_dataflow(
            job_name=JOB_NAME, variables=DATAFLOW_OPTIONS_JAVA,
            dataflow=JAR_FILE)
        EXPECTED_CMD = ['java', '-jar', JAR_FILE,
                        '--region=us-central1',
                        '--runner=DataflowRunner', '--project=test',
                        '--stagingLocation=gs://test/staging',
                        '--labels={"foo":"bar"}',
                        '--jobName={}-{}'.format(JOB_NAME, MOCK_UUID)]
        self.assertListEqual(sorted(mock_dataflow.call_args[0][0]),
                             sorted(EXPECTED_CMD))

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJob'))
    @mock.patch(DATAFLOW_STRING.format('_Dataflow'))
    @mock.patch(DATAFLOW_STRING.format('DataFlowHook.get_conn'))
    def test_start_java_dataflow_with_job_class(
            self, mock_conn, mock_dataflow, mock_dataflowjob, mock_uuid):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        self.dataflow_hook.start_java_dataflow(
            job_name=JOB_NAME, variables=DATAFLOW_OPTIONS_JAVA,
            dataflow=JAR_FILE, job_class=JOB_CLASS)
        EXPECTED_CMD = ['java', '-cp', JAR_FILE, JOB_CLASS,
                        '--region=us-central1',
                        '--runner=DataflowRunner', '--project=test',
                        '--stagingLocation=gs://test/staging',
                        '--labels={"foo":"bar"}',
                        '--jobName={}-{}'.format(JOB_NAME, MOCK_UUID)]
        self.assertListEqual(sorted(mock_dataflow.call_args[0][0]),
                             sorted(EXPECTED_CMD))

    @mock.patch('airflow.contrib.hooks.gcp_dataflow_hook._Dataflow.log')
    @mock.patch('subprocess.Popen')
    @mock.patch('select.select')
    def test_dataflow_wait_for_done_logging(self, mock_select, mock_popen, mock_logging):
        mock_logging.info = MagicMock()
        mock_logging.warning = MagicMock()
        mock_proc = MagicMock()
        mock_proc.stderr = MagicMock()
        mock_proc.stderr.readlines = MagicMock(return_value=['test\n', 'error\n'])
        mock_stderr_fd = MagicMock()
        mock_proc.stderr.fileno = MagicMock(return_value=mock_stderr_fd)
        mock_proc_poll = MagicMock()
        mock_select.return_value = [[mock_stderr_fd]]

        def poll_resp_error():
            mock_proc.return_code = 1
            return True
        mock_proc_poll.side_effect = [None, poll_resp_error]
        mock_proc.poll = mock_proc_poll
        mock_popen.return_value = mock_proc
        dataflow = _Dataflow(['test', 'cmd'])
        mock_logging.info.assert_called_with('Running command: %s', 'test cmd')
        self.assertRaises(Exception, dataflow.wait_for_done)

    def test_valid_dataflow_job_name(self):
        job_name = self.dataflow_hook._build_dataflow_job_name(
            job_name=JOB_NAME, append_job_name=False
        )

        self.assertEqual(job_name, JOB_NAME)

    def test_fix_underscore_in_job_name(self):
        job_name_with_underscore = 'test_example'
        fixed_job_name = job_name_with_underscore.replace(
            '_', '-'
        )
        job_name = self.dataflow_hook._build_dataflow_job_name(
            job_name=job_name_with_underscore, append_job_name=False
        )

        self.assertEqual(job_name, fixed_job_name)

    def test_invalid_dataflow_job_name(self):
        invalid_job_name = '9test_invalid_name'
        fixed_name = invalid_job_name.replace(
            '_', '-')

        with self.assertRaises(ValueError) as e:
            self.dataflow_hook._build_dataflow_job_name(
                job_name=invalid_job_name, append_job_name=False
            )
        #   Test whether the job_name is present in the Error msg
        self.assertIn('Invalid job_name ({})'.format(fixed_name),
                      str(e.exception))

    def test_dataflow_job_regex_check(self):

        self.assertEqual(self.dataflow_hook._build_dataflow_job_name(
            job_name='df-job-1', append_job_name=False
        ), 'df-job-1')

        self.assertEqual(self.dataflow_hook._build_dataflow_job_name(
            job_name='df-job', append_job_name=False
        ), 'df-job')

        self.assertEqual(self.dataflow_hook._build_dataflow_job_name(
            job_name='dfjob', append_job_name=False
        ), 'dfjob')

        self.assertEqual(self.dataflow_hook._build_dataflow_job_name(
            job_name='dfjob1', append_job_name=False
        ), 'dfjob1')

        self.assertRaises(
            ValueError,
            self.dataflow_hook._build_dataflow_job_name,
            job_name='1dfjob', append_job_name=False
        )

        self.assertRaises(
            ValueError,
            self.dataflow_hook._build_dataflow_job_name,
            job_name='dfjob@', append_job_name=False
        )

        self.assertRaises(
            ValueError,
            self.dataflow_hook._build_dataflow_job_name,
            job_name='df^jo', append_job_name=False
        )


class DataFlowTemplateHookTest(unittest.TestCase):

    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleCloudBaseHook.__init__'),
                        new=mock_init):
            self.dataflow_hook = DataFlowHook(gcp_conn_id='test')

    @mock.patch(DATAFLOW_STRING.format('DataFlowHook._start_template_dataflow'))
    def test_start_template_dataflow(self, internal_dataflow_mock):
        self.dataflow_hook.start_template_dataflow(
            job_name=JOB_NAME, variables=DATAFLOW_OPTIONS_TEMPLATE, parameters=PARAMETERS,
            dataflow_template=TEMPLATE)
        options_with_region = {'region': 'us-central1'}
        options_with_region.update(DATAFLOW_OPTIONS_TEMPLATE)
        internal_dataflow_mock.assert_called_once_with(
            mock.ANY, options_with_region, PARAMETERS, TEMPLATE)

    @mock.patch(DATAFLOW_STRING.format('_DataflowJob'))
    @mock.patch(DATAFLOW_STRING.format('DataFlowHook.get_conn'))
    def test_start_template_dataflow_with_runtime_env(self, mock_conn, mock_dataflowjob):
        dataflow_options_template = copy.deepcopy(DATAFLOW_OPTIONS_TEMPLATE)
        options_with_runtime_env = copy.deepcopy(RUNTIME_ENV)
        options_with_runtime_env.update(dataflow_options_template)

        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        method = (mock_conn.return_value
                  .projects.return_value
                  .locations.return_value
                  .templates.return_value
                  .launch)

        self.dataflow_hook.start_template_dataflow(
            job_name=JOB_NAME,
            variables=options_with_runtime_env,
            parameters=PARAMETERS,
            dataflow_template=TEMPLATE
        )
        body = {"jobName": mock.ANY,
                "parameters": PARAMETERS,
                "environment": RUNTIME_ENV
                }
        method.assert_called_once_with(
            projectId=options_with_runtime_env['project'],
            location='us-central1',
            gcsPath=TEMPLATE,
            body=body,
        )


class DataFlowJobTest(unittest.TestCase):

    def setUp(self):
        self.mock_dataflow = MagicMock()

    def test_dataflow_job_init_with_job_id(self):
        mock_jobs = MagicMock()
        self.mock_dataflow.projects.return_value.locations.return_value.\
            jobs.return_value = mock_jobs
        _DataflowJob(self.mock_dataflow, TEST_PROJECT, TEST_JOB_NAME,
                     TEST_LOCATION, 10, TEST_JOB_ID)
        mock_jobs.get.assert_called_with(projectId=TEST_PROJECT, location=TEST_LOCATION,
                                         jobId=TEST_JOB_ID)

    def test_dataflow_job_init_without_job_id(self):
        mock_jobs = MagicMock()
        self.mock_dataflow.projects.return_value.locations.return_value.\
            jobs.return_value = mock_jobs
        _DataflowJob(self.mock_dataflow, TEST_PROJECT, TEST_JOB_NAME,
                     TEST_LOCATION, 10)
        mock_jobs.list.assert_called_with(projectId=TEST_PROJECT,
                                          location=TEST_LOCATION)


class DataflowTest(unittest.TestCase):

    def test_data_flow_valid_job_id(self):
        cmd = ['echo', 'additional unit test lines.\n' +
               'INFO: the Dataflow monitoring console, please navigate to' +
               'https://console.cloud.google.com/dataflow/jobsDetail/locations/' +
               '{}/jobs/{}?project={}'.format(TEST_LOCATION, TEST_JOB_ID, TEST_PROJECT)]
        self.assertEqual(_Dataflow(cmd).wait_for_done(), TEST_JOB_ID)

    def test_data_flow_missing_job_id(self):
        cmd = ['echo', 'unit testing']
        self.assertEqual(_Dataflow(cmd).wait_for_done(), None)
