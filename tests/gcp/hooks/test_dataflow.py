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

from parameterized import parameterized

from airflow import AirflowException
from airflow.gcp.hooks.dataflow import (
    DataflowHook, DataflowJobStatus, DataflowJobType, _DataflowJobsController, _DataflowRunner,
    _fallback_to_project_id_from_variables,
)
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
BASE_STRING = 'airflow.gcp.hooks.base.{}'
DATAFLOW_STRING = 'airflow.gcp.hooks.dataflow.{}'
MOCK_UUID = '12345678'
TEST_PROJECT = 'test-project'
TEST_JOB_NAME = 'test-job-name'
TEST_JOB_ID = 'test-job-id'
TEST_LOCATION = 'us-central1'
DEFAULT_PY_INTERPRETER = 'python2'


class TestFallbackToVariables(unittest.TestCase):

    def test_support_project_id_parameter(self):
        mock_instance = mock.MagicMock()

        class FixtureFallback:
            @_fallback_to_project_id_from_variables
            def test_fn(self, *args, **kwargs):
                mock_instance(*args, **kwargs)

        FixtureFallback().test_fn(project_id="TEST")

        mock_instance.assert_called_once_with(project_id="TEST")

    def test_support_project_id_from_variable_parameter(self):
        mock_instance = mock.MagicMock()

        class FixtureFallback:
            @_fallback_to_project_id_from_variables
            def test_fn(self, *args, **kwargs):
                mock_instance(*args, **kwargs)

        FixtureFallback().test_fn(variables={'project': "TEST"})

        mock_instance.assert_called_once_with(project_id='TEST', variables={})

    def test_raise_exception_on_conflict(self):
        mock_instance = mock.MagicMock()

        class FixtureFallback:
            @_fallback_to_project_id_from_variables
            def test_fn(self, *args, **kwargs):
                mock_instance(*args, **kwargs)

        with self.assertRaisesRegex(
            AirflowException,
            "The mutually exclusive parameter `project_id` and `project` key in `variables` parameters are "
            "both present\\. Please remove one\\."
        ):
            FixtureFallback().test_fn(variables={'project': "TEST"}, project_id="TEST2")

    def test_raise_exception_on_positional_argument(self):
        mock_instance = mock.MagicMock()

        class FixutureFallback:
            @_fallback_to_project_id_from_variables
            def test_fn(self, *args, **kwargs):
                mock_instance(*args, **kwargs)

        with self.assertRaisesRegex(
            AirflowException,
            "You must use keyword arguments in this methods rather than positional"
        ):
            FixutureFallback().test_fn({'project': "TEST"}, "TEST2")


def mock_init(self, gcp_conn_id, delegate_to=None):  # pylint: disable=unused-argument
    pass


class TestDataflowHook(unittest.TestCase):

    def setUp(self):
        with mock.patch(BASE_STRING.format('CloudBaseHook.__init__'),
                        new=mock_init):
            self.dataflow_hook = DataflowHook(gcp_conn_id='test')

    @mock.patch("airflow.gcp.hooks.dataflow.DataflowHook._authorize")
    @mock.patch("airflow.gcp.hooks.dataflow.build")
    def test_dataflow_client_creation(self, mock_build, mock_authorize):
        result = self.dataflow_hook.get_conn()
        mock_build.assert_called_once_with(
            'dataflow', 'v1b3', http=mock_authorize.return_value, cache_discovery=False
        )
        self.assertEqual(mock_build.return_value, result)

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowRunner'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_python_dataflow(
        self, mock_conn, mock_dataflow, mock_dataflowjob, mock_uuid
    ):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        self.dataflow_hook.start_python_dataflow(
            job_name=JOB_NAME, variables=DATAFLOW_OPTIONS_PY,
            dataflow=PY_FILE, py_options=PY_OPTIONS)
        expected_cmd = ["python2", '-m', PY_FILE,
                        '--region=us-central1',
                        '--runner=DataflowRunner', '--project=test',
                        '--labels=foo=bar',
                        '--staging_location=gs://test/staging',
                        '--job_name={}-{}'.format(JOB_NAME, MOCK_UUID)]
        self.assertListEqual(sorted(mock_dataflow.call_args[0][0]),
                             sorted(expected_cmd))

    @parameterized.expand([
        ('default_to_python2', "python2"),
        ('major_version_2', 'python2'),
        ('major_version_3', 'python3'),
        ('minor_version', 'python3.6')
    ])
    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowRunner'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_python_dataflow_with_custom_interpreter(
        self, name, py_interpreter, mock_conn, mock_dataflow, mock_dataflowjob, mock_uuid
    ):
        del name  # unused variable
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        self.dataflow_hook.start_python_dataflow(
            job_name=JOB_NAME, variables=DATAFLOW_OPTIONS_PY,
            dataflow=PY_FILE, py_options=PY_OPTIONS,
            py_interpreter=py_interpreter)
        expected_cmd = [py_interpreter, '-m', PY_FILE,
                        '--region=us-central1',
                        '--runner=DataflowRunner', '--project=test',
                        '--labels=foo=bar',
                        '--staging_location=gs://test/staging',
                        '--job_name={}-{}'.format(JOB_NAME, MOCK_UUID)]
        self.assertListEqual(sorted(mock_dataflow.call_args[0][0]),
                             sorted(expected_cmd))

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowRunner'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
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
            jar=JAR_FILE)
        expected_cmd = ['java', '-jar', JAR_FILE,
                        '--region=us-central1',
                        '--runner=DataflowRunner', '--project=test',
                        '--stagingLocation=gs://test/staging',
                        '--labels={"foo":"bar"}',
                        '--jobName={}-{}'.format(JOB_NAME, MOCK_UUID)]
        self.assertListEqual(sorted(mock_dataflow.call_args[0][0]),
                             sorted(expected_cmd))

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowRunner'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_java_dataflow_with_job_class(self, mock_conn, mock_dataflow, mock_dataflowjob, mock_uuid):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        self.dataflow_hook.start_java_dataflow(
            job_name=JOB_NAME, variables=DATAFLOW_OPTIONS_JAVA,
            jar=JAR_FILE, job_class=JOB_CLASS)
        expected_cmd = ['java', '-cp', JAR_FILE, JOB_CLASS,
                        '--region=us-central1',
                        '--runner=DataflowRunner', '--project=test',
                        '--stagingLocation=gs://test/staging',
                        '--labels={"foo":"bar"}',
                        '--jobName={}-{}'.format(JOB_NAME, MOCK_UUID)]
        self.assertListEqual(sorted(mock_dataflow.call_args[0][0]),
                             sorted(expected_cmd))

    @parameterized.expand([
        (JOB_NAME, JOB_NAME, False),
        ('test-example', 'test_example', False),
        ('test-dataflow-pipeline-12345678', JOB_NAME, True),
        ('test-example-12345678', 'test_example', True),
        ('df-job-1', 'df-job-1', False),
        ('df-job', 'df-job', False),
        ('dfjob', 'dfjob', False),
        ('dfjob1', 'dfjob1', False),
    ])
    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'), return_value=MOCK_UUID)
    def test_valid_dataflow_job_name(self, expected_result, job_name, append_job_name, mock_uuid4):
        job_name = self.dataflow_hook._build_dataflow_job_name(
            job_name=job_name, append_job_name=append_job_name
        )

        self.assertEqual(expected_result, job_name)

    @parameterized.expand([
        ("1dfjob@", ),
        ("dfjob@", ),
        ("df^jo", )
    ])
    def test_build_dataflow_job_name_with_invalid_value(self, job_name):
        self.assertRaises(
            ValueError,
            self.dataflow_hook._build_dataflow_job_name,
            job_name=job_name, append_job_name=False
        )


class TestDataflowTemplateHook(unittest.TestCase):

    def setUp(self):
        with mock.patch(BASE_STRING.format('CloudBaseHook.__init__'),
                        new=mock_init):
            self.dataflow_hook = DataflowHook(gcp_conn_id='test')

    @mock.patch(DATAFLOW_STRING.format('DataflowHook._start_template_dataflow'))
    def test_start_template_dataflow(self, internal_dataflow_mock):
        self.dataflow_hook.start_template_dataflow(
            job_name=JOB_NAME, variables=DATAFLOW_OPTIONS_TEMPLATE, parameters=PARAMETERS,
            dataflow_template=TEMPLATE)
        options_with_region = {'region': 'us-central1'}
        options_with_region.update(DATAFLOW_OPTIONS_TEMPLATE)
        options_with_region_without_project = copy.deepcopy(options_with_region)
        del options_with_region_without_project['project']
        internal_dataflow_mock.assert_called_once_with(
            mock.ANY, options_with_region_without_project, PARAMETERS, TEMPLATE,
            DATAFLOW_OPTIONS_JAVA['project']
        )

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'), return_value=MOCK_UUID)
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_template_dataflow_with_runtime_env(self, mock_conn, mock_dataflowjob, mock_uuid):
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

        method.return_value.execute.return_value = {'job': {'id': TEST_JOB_ID}}
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
        mock_dataflowjob.assert_called_once_with(
            dataflow=mock_conn.return_value,
            job_id=TEST_JOB_ID,
            location='us-central1',
            name='test-dataflow-pipeline-{}'.format(MOCK_UUID),
            num_retries=5,
            poll_sleep=10,
            project_number='test'
        )
        mock_uuid.assert_called_once_with()


class TestDataflowJob(unittest.TestCase):

    def setUp(self):
        self.mock_dataflow = MagicMock()

    def test_dataflow_job_init_with_job_id(self):
        mock_jobs = MagicMock()
        self.mock_dataflow.projects.return_value.locations.return_value. \
            jobs.return_value = mock_jobs
        _DataflowJobsController(
            self.mock_dataflow, TEST_PROJECT, TEST_JOB_NAME,
            TEST_LOCATION, 10, TEST_JOB_ID).get_jobs()
        mock_jobs.get.assert_called_once_with(projectId=TEST_PROJECT, location=TEST_LOCATION,
                                              jobId=TEST_JOB_ID)

    def test_dataflow_job_init_without_job_id(self):
        job = {"id": TEST_JOB_ID, "name": TEST_JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_DONE}

        mock_list = (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.list
        )
        (
            mock_list.return_value.
            execute.return_value
        ) = {'jobs': [job]}
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None
        _DataflowJobsController(
            self.mock_dataflow, TEST_PROJECT, TEST_JOB_NAME,
            TEST_LOCATION, 10).get_jobs()

        mock_list.assert_called_once_with(
            projectId=TEST_PROJECT,
            location=TEST_LOCATION
        )

    def test_dataflow_job_wait_for_multiple_jobs(self):
        job = {"id": TEST_JOB_ID, "name": TEST_JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_DONE}

        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list.return_value.
            execute.return_value
        ) = {
            "jobs": [job, job]
        }
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None

        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name=TEST_JOB_NAME,
            location=TEST_LOCATION,
            poll_sleep=10,
            job_id=TEST_JOB_ID,
            num_retries=20,
            multiple_jobs=True
        )
        dataflow_job.wait_for_done()

        self.mock_dataflow.projects.return_value.locations.return_value. \
            jobs.return_value.list.assert_called_once_with(location=TEST_LOCATION, projectId=TEST_PROJECT)

        self.mock_dataflow.projects.return_value.locations.return_value. \
            jobs.return_value.list.return_value.execute.assert_called_once_with(num_retries=20)

        self.assertEqual(dataflow_job.get_jobs(), [job, job])

    def test_dataflow_job_wait_for_multiple_jobs_and_one_failed(self):
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list.return_value.
            execute.return_value
        ) = {
            "jobs": [
                {"id": "id-1", "name": "name-1", "currentState": DataflowJobStatus.JOB_STATE_DONE},
                {"id": "id-2", "name": "name-2", "currentState": DataflowJobStatus.JOB_STATE_FAILED}
            ]
        }
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None

        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name="name-",
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=None,
            num_retries=20,
            multiple_jobs=True
        )
        with self.assertRaisesRegex(Exception, 'Google Cloud Dataflow job name-2 has failed\\.'):
            dataflow_job.wait_for_done()

    def test_dataflow_job_wait_for_multiple_jobs_and_one_cancelled(self):
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list.return_value.
            execute.return_value
        ) = {
            "jobs": [
                {"id": "id-1", "name": "name-1", "currentState": DataflowJobStatus.JOB_STATE_DONE},
                {"id": "id-2", "name": "name-2", "currentState": DataflowJobStatus.JOB_STATE_CANCELLED}
            ]
        }
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None

        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name="name-",
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=None,
            num_retries=20,
            multiple_jobs=True
        )
        with self.assertRaisesRegex(Exception, 'Google Cloud Dataflow job name-2 was cancelled\\.'):
            dataflow_job.wait_for_done()

    def test_dataflow_job_wait_for_multiple_jobs_and_one_unknown(self):
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list.return_value.
            execute.return_value
        ) = {
            "jobs": [
                {"id": "id-1", "name": "name-1", "currentState": DataflowJobStatus.JOB_STATE_DONE},
                {"id": "id-2", "name": "name-2", "currentState": "unknown"}
            ]
        }
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None

        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name="name-",
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=None,
            num_retries=20,
            multiple_jobs=True
        )
        with self.assertRaisesRegex(Exception, 'Google Cloud Dataflow job name-2 was unknown state: unknown'):
            dataflow_job.wait_for_done()

    def test_dataflow_job_wait_for_multiple_jobs_and_streaming_jobs(self):
        mock_jobs_list = (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list
        )
        mock_jobs_list.return_value.execute.return_value = {
            "jobs": [
                {
                    "id": "id-2",
                    "name": "name-2",
                    "currentState": DataflowJobStatus.JOB_STATE_RUNNING,
                    "type": DataflowJobType.JOB_TYPE_STREAMING
                }
            ]
        }
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None

        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name="name-",
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=None,
            num_retries=20,
            multiple_jobs=True
        )
        dataflow_job.wait_for_done()

        self.assertEqual(1, mock_jobs_list.call_count)

    def test_dataflow_job_wait_for_single_jobs(self):
        job = {"id": TEST_JOB_ID, "name": TEST_JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_DONE}

        self.mock_dataflow.projects.return_value.locations.return_value. \
            jobs.return_value.get.return_value.execute.return_value = job

        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None

        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name=TEST_JOB_NAME,
            location=TEST_LOCATION,
            poll_sleep=10,
            job_id=TEST_JOB_ID,
            num_retries=20,
            multiple_jobs=False
        )
        dataflow_job.wait_for_done()

        self.mock_dataflow.projects.return_value.locations.return_value. \
            jobs.return_value.get.assert_called_once_with(
                jobId=TEST_JOB_ID,
                location=TEST_LOCATION,
                projectId=TEST_PROJECT
            )

        self.mock_dataflow.projects.return_value.locations.return_value. \
            jobs.return_value.get.return_value.execute.assert_called_once_with(num_retries=20)

        self.assertEqual(dataflow_job.get_jobs(), [job])


class TestDataflow(unittest.TestCase):

    def test_data_flow_valid_job_id(self):
        cmd = [
            'echo', 'additional unit test lines.\n' +
            'https://console.cloud.google.com/dataflow/jobsDetail/locations/us-central1/'
            'jobs/{}?project=XXX'.format(TEST_JOB_ID)
        ]
        self.assertEqual(_DataflowRunner(cmd).wait_for_done(), TEST_JOB_ID)

    def test_data_flow_missing_job_id(self):
        cmd = ['echo', 'unit testing']
        self.assertEqual(_DataflowRunner(cmd).wait_for_done(), None)

    @mock.patch('airflow.gcp.hooks.dataflow._DataflowRunner.log')
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
        dataflow = _DataflowRunner(['test', 'cmd'])
        mock_logging.info.assert_called_once_with('Running command: %s', 'test cmd')
        self.assertRaises(Exception, dataflow.wait_for_done)
