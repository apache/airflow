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
import shlex
import unittest
from typing import Any, Dict

import mock
from mock import MagicMock
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataflow import (
    DEFAULT_DATAFLOW_LOCATION,
    DataflowHook,
    DataflowJobStatus,
    DataflowJobType,
    _DataflowJobsController,
    _DataflowRunner,
    _fallback_to_project_id_from_variables,
)

TASK_ID = 'test-dataflow-operator'
JOB_NAME = 'test-dataflow-pipeline'
MOCK_UUID = '12345678'
UNIQUE_JOB_NAME = 'test-dataflow-pipeline-{}'.format(MOCK_UUID)
TEST_TEMPLATE = 'gs://dataflow-templates/wordcount/template_file'
PARAMETERS = {
    'inputFile': 'gs://dataflow-samples/shakespeare/kinglear.txt',
    'output': 'gs://test/output/my_output',
}
PY_FILE = 'apache_beam.examples.wordcount'
JAR_FILE = 'unitest.jar'
JOB_CLASS = 'com.example.UnitTest'
PY_OPTIONS = ['-m']
DATAFLOW_VARIABLES_PY = {'project': 'test', 'staging_location': 'gs://test/staging', 'labels': {'foo': 'bar'}}
DATAFLOW_VARIABLES_JAVA = {
    'project': 'test',
    'stagingLocation': 'gs://test/staging',
    'labels': {'foo': 'bar'},
}
RUNTIME_ENV = {
    'additionalExperiments': ['exp_flag1', 'exp_flag2'],
    'additionalUserLabels': {'name': 'wrench', 'mass': '1.3kg', 'count': '3'},
    'bypassTempDirValidation': {},
    'ipConfiguration': 'WORKER_IP_PRIVATE',
    'kmsKeyName': (
        'projects/TEST_PROJECT_ID/locations/TEST_LOCATIONS/keyRings/TEST_KEYRING/cryptoKeys/TEST_CRYPTOKEYS'
    ),
    'maxWorkers': 10,
    'network': 'default',
    'numWorkers': 2,
    'serviceAccountEmail': 'test@apache.airflow',
    'subnetwork': 'regions/REGION/subnetworks/SUBNETWORK',
    'tempLocation': 'gs://test/temp',
    'workerRegion': "test-region",
    'workerZone': 'test-zone',
    'zone': 'us-central1-f',
    'machineType': 'n1-standard-1',
}
BASE_STRING = 'airflow.providers.google.common.hooks.base_google.{}'
DATAFLOW_STRING = 'airflow.providers.google.cloud.hooks.dataflow.{}'
TEST_PROJECT = 'test-project'
TEST_JOB_ID = 'test-job-id'
TEST_LOCATION = 'custom-location'
DEFAULT_PY_INTERPRETER = 'python3'


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
            "The mutually exclusive parameter `project_id` and `project` key in `variables` parameter are "
            "both present\\. Please remove one\\.",
        ):
            FixtureFallback().test_fn(variables={'project': "TEST"}, project_id="TEST2")

    def test_raise_exception_on_positional_argument(self):
        mock_instance = mock.MagicMock()

        class FixutureFallback:
            @_fallback_to_project_id_from_variables
            def test_fn(self, *args, **kwargs):
                mock_instance(*args, **kwargs)

        with self.assertRaisesRegex(
            AirflowException, "You must use keyword arguments in this methods rather than positional"
        ):
            FixutureFallback().test_fn({'project': "TEST"}, "TEST2")


def mock_init(
    self,
    gcp_conn_id,
    delegate_to=None,
    impersonation_chain=None,
):  # pylint: disable=unused-argument
    pass


class TestDataflowHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleBaseHook.__init__'), new=mock_init):
            self.dataflow_hook = DataflowHook(gcp_conn_id='test')

    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.DataflowHook._authorize")
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.build")
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
    def test_start_python_dataflow(self, mock_conn, mock_dataflow, mock_dataflowjob, mock_uuid):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        self.dataflow_hook.start_python_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables=DATAFLOW_VARIABLES_PY,
            dataflow=PY_FILE,
            py_options=PY_OPTIONS,
        )
        expected_cmd = [
            "python3",
            '-m',
            PY_FILE,
            '--region=us-central1',
            '--runner=DataflowRunner',
            '--project=test',
            '--labels=foo=bar',
            '--staging_location=gs://test/staging',
            '--job_name={}-{}'.format(JOB_NAME, MOCK_UUID),
        ]
        self.assertListEqual(sorted(mock_dataflow.call_args[1]["cmd"]), sorted(expected_cmd))

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowRunner'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_python_dataflow_with_custom_region_as_variable(
        self, mock_conn, mock_dataflow, mock_dataflowjob, mock_uuid
    ):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        variables = copy.deepcopy(DATAFLOW_VARIABLES_PY)
        variables['region'] = TEST_LOCATION
        self.dataflow_hook.start_python_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables=variables,
            dataflow=PY_FILE,
            py_options=PY_OPTIONS,
        )
        expected_cmd = [
            "python3",
            '-m',
            PY_FILE,
            f'--region={TEST_LOCATION}',
            '--runner=DataflowRunner',
            '--project=test',
            '--labels=foo=bar',
            '--staging_location=gs://test/staging',
            '--job_name={}-{}'.format(JOB_NAME, MOCK_UUID),
        ]
        self.assertListEqual(sorted(mock_dataflow.call_args[1]["cmd"]), sorted(expected_cmd))

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowRunner'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_python_dataflow_with_custom_region_as_paramater(
        self, mock_conn, mock_dataflow, mock_dataflowjob, mock_uuid
    ):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        self.dataflow_hook.start_python_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables=DATAFLOW_VARIABLES_PY,
            dataflow=PY_FILE,
            py_options=PY_OPTIONS,
            location=TEST_LOCATION,
        )
        expected_cmd = [
            "python3",
            '-m',
            PY_FILE,
            f'--region={TEST_LOCATION}',
            '--runner=DataflowRunner',
            '--project=test',
            '--labels=foo=bar',
            '--staging_location=gs://test/staging',
            '--job_name={}-{}'.format(JOB_NAME, MOCK_UUID),
        ]
        self.assertListEqual(sorted(mock_dataflow.call_args[1]["cmd"]), sorted(expected_cmd))

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowRunner'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_python_dataflow_with_multiple_extra_packages(
        self, mock_conn, mock_dataflow, mock_dataflowjob, mock_uuid
    ):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        variables: Dict[str, Any] = copy.deepcopy(DATAFLOW_VARIABLES_PY)
        variables['extra-package'] = ['a.whl', 'b.whl']

        self.dataflow_hook.start_python_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables=variables,
            dataflow=PY_FILE,
            py_options=PY_OPTIONS,
        )
        expected_cmd = [
            "python3",
            '-m',
            PY_FILE,
            '--extra-package=a.whl',
            '--extra-package=b.whl',
            '--region=us-central1',
            '--runner=DataflowRunner',
            '--project=test',
            '--labels=foo=bar',
            '--staging_location=gs://test/staging',
            '--job_name={}-{}'.format(JOB_NAME, MOCK_UUID),
        ]
        self.assertListEqual(sorted(mock_dataflow.call_args[1]["cmd"]), sorted(expected_cmd))

    @parameterized.expand(
        [
            ('default_to_python3', 'python3'),
            ('major_version_2', 'python2'),
            ('major_version_3', 'python3'),
            ('minor_version', 'python3.6'),
        ]
    )
    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowRunner'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_python_dataflow_with_custom_interpreter(
        self,
        name,
        py_interpreter,
        mock_conn,
        mock_dataflow,
        mock_dataflowjob,
        mock_uuid,
    ):
        del name  # unused variable
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        self.dataflow_hook.start_python_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables=DATAFLOW_VARIABLES_PY,
            dataflow=PY_FILE,
            py_options=PY_OPTIONS,
            py_interpreter=py_interpreter,
        )
        expected_cmd = [
            py_interpreter,
            '-m',
            PY_FILE,
            '--region=us-central1',
            '--runner=DataflowRunner',
            '--project=test',
            '--labels=foo=bar',
            '--staging_location=gs://test/staging',
            '--job_name={}-{}'.format(JOB_NAME, MOCK_UUID),
        ]
        self.assertListEqual(sorted(mock_dataflow.call_args[1]["cmd"]), sorted(expected_cmd))

    @parameterized.expand(
        [
            (['foo-bar'], False),
            (['foo-bar'], True),
            ([], True),
        ]
    )
    @mock.patch(DATAFLOW_STRING.format('prepare_virtualenv'))
    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowRunner'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_python_dataflow_with_non_empty_py_requirements_and_without_system_packages(
        self,
        current_py_requirements,
        current_py_system_site_packages,
        mock_conn,
        mock_dataflow,
        mock_dataflowjob,
        mock_uuid,
        mock_virtualenv,
    ):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        mock_virtualenv.return_value = '/dummy_dir/bin/python'
        self.dataflow_hook.start_python_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables=DATAFLOW_VARIABLES_PY,
            dataflow=PY_FILE,
            py_options=PY_OPTIONS,
            py_requirements=current_py_requirements,
            py_system_site_packages=current_py_system_site_packages,
        )
        expected_cmd = [
            '/dummy_dir/bin/python',
            '-m',
            PY_FILE,
            '--region=us-central1',
            '--runner=DataflowRunner',
            '--project=test',
            '--labels=foo=bar',
            '--staging_location=gs://test/staging',
            '--job_name={}-{}'.format(JOB_NAME, MOCK_UUID),
        ]
        self.assertListEqual(sorted(mock_dataflow.call_args[1]["cmd"]), sorted(expected_cmd))

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowRunner'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_python_dataflow_with_empty_py_requirements_and_without_system_packages(
        self, mock_conn, mock_dataflow, mock_dataflowjob, mock_uuid
    ):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        with self.assertRaisesRegex(AirflowException, "Invalid method invocation."):
            self.dataflow_hook.start_python_dataflow(  # pylint: disable=no-value-for-parameter
                job_name=JOB_NAME,
                variables=DATAFLOW_VARIABLES_PY,
                dataflow=PY_FILE,
                py_options=PY_OPTIONS,
                py_requirements=[],
            )

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowRunner'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_java_dataflow(self, mock_conn, mock_dataflow, mock_dataflowjob, mock_uuid):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        self.dataflow_hook.start_java_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME, variables=DATAFLOW_VARIABLES_JAVA, jar=JAR_FILE
        )
        expected_cmd = [
            'java',
            '-jar',
            JAR_FILE,
            '--region=us-central1',
            '--runner=DataflowRunner',
            '--project=test',
            '--stagingLocation=gs://test/staging',
            '--labels={"foo":"bar"}',
            '--jobName={}-{}'.format(JOB_NAME, MOCK_UUID),
        ]
        self.assertListEqual(
            sorted(expected_cmd),
            sorted(mock_dataflow.call_args[1]["cmd"]),
        )

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowRunner'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_java_dataflow_with_multiple_values_in_variables(
        self, mock_conn, mock_dataflow, mock_dataflowjob, mock_uuid
    ):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        variables: Dict[str, Any] = copy.deepcopy(DATAFLOW_VARIABLES_JAVA)
        variables['mock-option'] = ['a.whl', 'b.whl']

        self.dataflow_hook.start_java_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME, variables=variables, jar=JAR_FILE
        )
        expected_cmd = [
            'java',
            '-jar',
            JAR_FILE,
            '--mock-option=a.whl',
            '--mock-option=b.whl',
            '--region=us-central1',
            '--runner=DataflowRunner',
            '--project=test',
            '--stagingLocation=gs://test/staging',
            '--labels={"foo":"bar"}',
            '--jobName={}-{}'.format(JOB_NAME, MOCK_UUID),
        ]
        self.assertListEqual(sorted(mock_dataflow.call_args[1]["cmd"]), sorted(expected_cmd))

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowRunner'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_java_dataflow_with_custom_region_as_variable(
        self, mock_conn, mock_dataflow, mock_dataflowjob, mock_uuid
    ):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None

        variables = copy.deepcopy(DATAFLOW_VARIABLES_JAVA)
        variables['region'] = TEST_LOCATION

        self.dataflow_hook.start_java_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME, variables=variables, jar=JAR_FILE
        )
        expected_cmd = [
            'java',
            '-jar',
            JAR_FILE,
            f'--region={TEST_LOCATION}',
            '--runner=DataflowRunner',
            '--project=test',
            '--stagingLocation=gs://test/staging',
            '--labels={"foo":"bar"}',
            '--jobName={}-{}'.format(JOB_NAME, MOCK_UUID),
        ]
        self.assertListEqual(
            sorted(expected_cmd),
            sorted(mock_dataflow.call_args[1]["cmd"]),
        )

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('_DataflowRunner'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_java_dataflow_with_custom_region_as_parameter(
        self, mock_conn, mock_dataflow, mock_dataflowjob, mock_uuid
    ):
        mock_uuid.return_value = MOCK_UUID
        mock_conn.return_value = None
        dataflow_instance = mock_dataflow.return_value
        dataflow_instance.wait_for_done.return_value = None
        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None

        variables = copy.deepcopy(DATAFLOW_VARIABLES_JAVA)
        variables['region'] = TEST_LOCATION

        self.dataflow_hook.start_java_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME, variables=variables, jar=JAR_FILE
        )
        expected_cmd = [
            'java',
            '-jar',
            JAR_FILE,
            f'--region={TEST_LOCATION}',
            '--runner=DataflowRunner',
            '--project=test',
            '--stagingLocation=gs://test/staging',
            '--labels={"foo":"bar"}',
            '--jobName={}-{}'.format(JOB_NAME, MOCK_UUID),
        ]
        self.assertListEqual(
            sorted(expected_cmd),
            sorted(mock_dataflow.call_args[1]["cmd"]),
        )

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
        self.dataflow_hook.start_java_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME, variables=DATAFLOW_VARIABLES_JAVA, jar=JAR_FILE, job_class=JOB_CLASS
        )
        expected_cmd = [
            'java',
            '-cp',
            JAR_FILE,
            JOB_CLASS,
            '--region=us-central1',
            '--runner=DataflowRunner',
            '--project=test',
            '--stagingLocation=gs://test/staging',
            '--labels={"foo":"bar"}',
            '--jobName={}-{}'.format(JOB_NAME, MOCK_UUID),
        ]
        self.assertListEqual(sorted(mock_dataflow.call_args[1]["cmd"]), sorted(expected_cmd))

    @parameterized.expand(
        [
            (JOB_NAME, JOB_NAME, False),
            ('test-example', 'test_example', False),
            ('test-dataflow-pipeline-12345678', JOB_NAME, True),
            ('test-example-12345678', 'test_example', True),
            ('df-job-1', 'df-job-1', False),
            ('df-job', 'df-job', False),
            ('dfjob', 'dfjob', False),
            ('dfjob1', 'dfjob1', False),
        ]
    )
    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'), return_value=MOCK_UUID)
    def test_valid_dataflow_job_name(self, expected_result, job_name, append_job_name, mock_uuid4):
        job_name = self.dataflow_hook._build_dataflow_job_name(
            job_name=job_name, append_job_name=append_job_name
        )

        self.assertEqual(expected_result, job_name)

    @parameterized.expand([("1dfjob@",), ("dfjob@",), ("df^jo",)])
    def test_build_dataflow_job_name_with_invalid_value(self, job_name):
        self.assertRaises(
            ValueError, self.dataflow_hook._build_dataflow_job_name, job_name=job_name, append_job_name=False
        )


class TestDataflowTemplateHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleBaseHook.__init__'), new=mock_init):
            self.dataflow_hook = DataflowHook(gcp_conn_id='test')

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'), return_value=MOCK_UUID)
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_template_dataflow(self, mock_conn, mock_controller, mock_uuid):

        launch_method = (
            mock_conn.return_value.projects.return_value.locations.return_value.templates.return_value.launch
        )
        launch_method.return_value.execute.return_value = {"job": {"id": TEST_JOB_ID}}
        variables = {'zone': 'us-central1-f', 'tempLocation': 'gs://test/temp'}
        self.dataflow_hook.start_template_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables=copy.deepcopy(variables),
            parameters=PARAMETERS,
            dataflow_template=TEST_TEMPLATE,
            project_id=TEST_PROJECT,
        )

        launch_method.assert_called_once_with(
            body={
                'jobName': 'test-dataflow-pipeline-12345678',
                'parameters': PARAMETERS,
                'environment': variables,
            },
            gcsPath='gs://dataflow-templates/wordcount/template_file',
            projectId=TEST_PROJECT,
            location=DEFAULT_DATAFLOW_LOCATION,
        )

        mock_controller.assert_called_once_with(
            dataflow=mock_conn.return_value,
            job_id='test-job-id',
            name='test-dataflow-pipeline-12345678',
            num_retries=5,
            poll_sleep=10,
            project_number=TEST_PROJECT,
            location=DEFAULT_DATAFLOW_LOCATION,
        )
        mock_controller.return_value.wait_for_done.assert_called_once()

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'), return_value=MOCK_UUID)
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_template_dataflow_with_custom_region_as_variable(
        self, mock_conn, mock_controller, mock_uuid
    ):
        launch_method = (
            mock_conn.return_value.projects.return_value.locations.return_value.templates.return_value.launch
        )
        launch_method.return_value.execute.return_value = {"job": {"id": TEST_JOB_ID}}
        self.dataflow_hook.start_template_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables={'region': TEST_LOCATION},
            parameters=PARAMETERS,
            dataflow_template=TEST_TEMPLATE,
            project_id=TEST_PROJECT,
        )

        launch_method.assert_called_once_with(
            projectId=TEST_PROJECT,
            location=TEST_LOCATION,
            gcsPath=TEST_TEMPLATE,
            body=mock.ANY,
        )

        mock_controller.assert_called_once_with(
            dataflow=mock_conn.return_value,
            job_id=TEST_JOB_ID,
            name=UNIQUE_JOB_NAME,
            num_retries=5,
            poll_sleep=10,
            project_number=TEST_PROJECT,
            location=TEST_LOCATION,
        )
        mock_controller.return_value.wait_for_done.assert_called_once()

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'), return_value=MOCK_UUID)
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_template_dataflow_with_custom_region_as_parameter(
        self, mock_conn, mock_controller, mock_uuid
    ):
        launch_method = (
            mock_conn.return_value.projects.return_value.locations.return_value.templates.return_value.launch
        )
        launch_method.return_value.execute.return_value = {"job": {"id": TEST_JOB_ID}}

        self.dataflow_hook.start_template_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables={},
            parameters=PARAMETERS,
            dataflow_template=TEST_TEMPLATE,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT,
        )

        launch_method.assert_called_once_with(
            body={'jobName': UNIQUE_JOB_NAME, 'parameters': PARAMETERS, 'environment': {}},
            gcsPath='gs://dataflow-templates/wordcount/template_file',
            projectId=TEST_PROJECT,
            location=TEST_LOCATION,
        )

        mock_controller.assert_called_once_with(
            dataflow=mock_conn.return_value,
            job_id=TEST_JOB_ID,
            name=UNIQUE_JOB_NAME,
            num_retries=5,
            poll_sleep=10,
            project_number=TEST_PROJECT,
            location=TEST_LOCATION,
        )
        mock_controller.return_value.wait_for_done.assert_called_once()

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'), return_value=MOCK_UUID)
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_template_dataflow_with_runtime_env(self, mock_conn, mock_dataflowjob, mock_uuid):
        options_with_runtime_env = copy.deepcopy(RUNTIME_ENV)

        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        # fmt: off
        method = (mock_conn.return_value
                  .projects.return_value
                  .locations.return_value
                  .templates.return_value
                  .launch)
        # fmt: on
        method.return_value.execute.return_value = {'job': {'id': TEST_JOB_ID}}
        self.dataflow_hook.start_template_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables=options_with_runtime_env,
            parameters=PARAMETERS,
            dataflow_template=TEST_TEMPLATE,
            project_id=TEST_PROJECT,
        )
        body = {"jobName": mock.ANY, "parameters": PARAMETERS, "environment": RUNTIME_ENV}
        method.assert_called_once_with(
            projectId=TEST_PROJECT,
            location=DEFAULT_DATAFLOW_LOCATION,
            gcsPath=TEST_TEMPLATE,
            body=body,
        )
        mock_dataflowjob.assert_called_once_with(
            dataflow=mock_conn.return_value,
            job_id=TEST_JOB_ID,
            location=DEFAULT_DATAFLOW_LOCATION,
            name='test-dataflow-pipeline-{}'.format(MOCK_UUID),
            num_retries=5,
            poll_sleep=10,
            project_number=TEST_PROJECT,
        )
        mock_uuid.assert_called_once_with()

    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_cancel_job(self, mock_get_conn, jobs_controller):
        self.dataflow_hook.cancel_job(
            job_name=UNIQUE_JOB_NAME, job_id=TEST_JOB_ID, project_id=TEST_PROJECT, location=TEST_LOCATION
        )
        jobs_controller.assert_called_once_with(
            dataflow=mock_get_conn.return_value,
            job_id=TEST_JOB_ID,
            location=TEST_LOCATION,
            name=UNIQUE_JOB_NAME,
            poll_sleep=10,
            project_number=TEST_PROJECT,
        )
        jobs_controller.cancel()


class TestDataflowJob(unittest.TestCase):
    def setUp(self):
        self.mock_dataflow = MagicMock()

    def test_dataflow_job_init_with_job_id(self):
        mock_jobs = MagicMock()
        self.mock_dataflow.projects.return_value.locations.return_value.jobs.return_value = mock_jobs
        _DataflowJobsController(
            self.mock_dataflow, TEST_PROJECT, TEST_LOCATION, 10, UNIQUE_JOB_NAME, TEST_JOB_ID
        ).get_jobs()
        mock_jobs.get.assert_called_once_with(
            projectId=TEST_PROJECT, location=TEST_LOCATION, jobId=TEST_JOB_ID
        )

    def test_dataflow_job_init_without_job_id(self):
        job = {"id": TEST_JOB_ID, "name": UNIQUE_JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_DONE}

        mock_list = self.mock_dataflow.projects.return_value.locations.return_value.jobs.return_value.list
        (mock_list.return_value.execute.return_value) = {'jobs': [job]}
        # fmt: off
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None
        # fmt: on
        _DataflowJobsController(
            self.mock_dataflow, TEST_PROJECT, TEST_LOCATION, 10, UNIQUE_JOB_NAME
        ).get_jobs()

        mock_list.assert_called_once_with(projectId=TEST_PROJECT, location=TEST_LOCATION)

    def test_dataflow_job_wait_for_multiple_jobs(self):
        job = {"id": TEST_JOB_ID, "name": UNIQUE_JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_DONE}
        # fmt: off
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
        # fmt: on
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name=UNIQUE_JOB_NAME,
            location=TEST_LOCATION,
            poll_sleep=10,
            job_id=TEST_JOB_ID,
            num_retries=20,
            multiple_jobs=True,
        )
        dataflow_job.wait_for_done()

        # fmt: off
        self.mock_dataflow.projects.return_value.locations.return_value.jobs.return_value.\
            list.assert_called_once_with(location=TEST_LOCATION, projectId=TEST_PROJECT)

        self.mock_dataflow.projects.return_value.locations.return_value.jobs.return_value.list\
            .return_value.execute.assert_called_once_with(num_retries=20)
        # fmt: on

        self.assertEqual(dataflow_job.get_jobs(), [job, job])

    def test_dataflow_job_wait_for_multiple_jobs_and_one_failed(self):
        # fmt: off
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
        # fmt: on
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name="name-",
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=None,
            num_retries=20,
            multiple_jobs=True,
        )
        with self.assertRaisesRegex(Exception, 'Google Cloud Dataflow job name-2 has failed\\.'):
            dataflow_job.wait_for_done()

    def test_dataflow_job_wait_for_multiple_jobs_and_one_cancelled(self):
        # fmt: off
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
        # fmt: on
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name="name-",
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=None,
            num_retries=20,
            multiple_jobs=True,
        )
        with self.assertRaisesRegex(Exception, 'Google Cloud Dataflow job name-2 was cancelled\\.'):
            dataflow_job.wait_for_done()

    def test_dataflow_job_wait_for_multiple_jobs_and_one_unknown(self):
        # fmt: off
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
        # fmt: on
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name="name-",
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=None,
            num_retries=20,
            multiple_jobs=True,
        )
        with self.assertRaisesRegex(Exception, 'Google Cloud Dataflow job name-2 was unknown state: unknown'):
            dataflow_job.wait_for_done()

    def test_dataflow_job_wait_for_multiple_jobs_and_streaming_jobs(self):
        # fmt: off
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
        # fmt: on
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name="name-",
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=None,
            num_retries=20,
            multiple_jobs=True,
        )
        dataflow_job.wait_for_done()

        self.assertEqual(1, mock_jobs_list.call_count)

    def test_dataflow_job_wait_for_single_jobs(self):
        job = {"id": TEST_JOB_ID, "name": UNIQUE_JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_DONE}
        # fmt: off
        self.mock_dataflow.projects.return_value.locations.return_value. \
            jobs.return_value.get.return_value.execute.return_value = job

        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None
        # fmt: on
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name=UNIQUE_JOB_NAME,
            location=TEST_LOCATION,
            poll_sleep=10,
            job_id=TEST_JOB_ID,
            num_retries=20,
            multiple_jobs=False,
        )
        dataflow_job.wait_for_done()
        # fmt: off
        self.mock_dataflow.projects.return_value.locations.return_value. \
            jobs.return_value.get.assert_called_once_with(
                jobId=TEST_JOB_ID,
                location=TEST_LOCATION,
                projectId=TEST_PROJECT
            )

        self.mock_dataflow.projects.return_value.locations.return_value. \
            jobs.return_value.get.return_value.execute.assert_called_once_with(num_retries=20)
        # fmt: on
        self.assertEqual(dataflow_job.get_jobs(), [job])

    def test_dataflow_job_is_job_running_with_no_job(self):
        # fmt: off
        mock_jobs_list = (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list
        )
        mock_jobs_list.return_value.execute.return_value = {
            "jobs": []
        }
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None
        # fmt: on
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name="name-",
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=None,
            num_retries=20,
            multiple_jobs=True,
        )
        result = dataflow_job.is_job_running()

        self.assertEqual(False, result)

    def test_dataflow_job_cancel_job(self):
        job = {
            "id": TEST_JOB_ID,
            "name": UNIQUE_JOB_NAME,
            "currentState": DataflowJobStatus.JOB_STATE_RUNNING,
        }
        # fmt: off
        get_method = (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            get
        )
        get_method.return_value.execute.return_value = job

        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None
        # fmt: on
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name=UNIQUE_JOB_NAME,
            location=TEST_LOCATION,
            poll_sleep=10,
            job_id=TEST_JOB_ID,
            num_retries=20,
            multiple_jobs=False,
        )
        dataflow_job.cancel()

        get_method.assert_called_once_with(jobId=TEST_JOB_ID, location=TEST_LOCATION, projectId=TEST_PROJECT)

        get_method.return_value.execute.assert_called_once_with(num_retries=20)

        self.mock_dataflow.new_batch_http_request.assert_called_once_with()

        mock_batch = self.mock_dataflow.new_batch_http_request.return_value
        # fmt: off
        mock_update = (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            update
        )
        # fmt: on
        mock_update.assert_called_once_with(
            body={'requestedState': 'JOB_STATE_CANCELLED'},
            jobId='test-job-id',
            location=TEST_LOCATION,
            projectId='test-project',
        )
        mock_batch.add.assert_called_once_with(mock_update.return_value)
        mock_batch.execute.assert_called_once()


APACHE_BEAM_V_2_14_0_JAVA_SDK_LOG = f""""\
Dataflow SDK version: 2.14.0
Jun 15, 2020 2:57:28 PM org.apache.beam.runners.dataflow.DataflowRunner run
INFO: To access the Dataflow monitoring console, please navigate to https://console.cloud.google.com/dataflow\
/jobsDetail/locations/europe-west3/jobs/{TEST_JOB_ID}?project=XXX
Submitted job: {TEST_JOB_ID}
Jun 15, 2020 2:57:28 PM org.apache.beam.runners.dataflow.DataflowRunner run
INFO: To cancel the job using the 'gcloud' tool, run:
> gcloud dataflow jobs --project=XXX cancel --region=europe-west3 {TEST_JOB_ID}
"""

APACHE_BEAM_V_2_22_0_JAVA_SDK_LOG = f""""\
INFO: Dataflow SDK version: 2.22.0
Jun 15, 2020 3:09:03 PM org.apache.beam.runners.dataflow.DataflowRunner run
INFO: To access the Dataflow monitoring console, please navigate to https://console.cloud.google.com/dataflow\
/jobs/europe-west3/{TEST_JOB_ID}?project=XXXX
Jun 15, 2020 3:09:03 PM org.apache.beam.runners.dataflow.DataflowRunner run
INFO: Submitted job: {TEST_JOB_ID}
Jun 15, 2020 3:09:03 PM org.apache.beam.runners.dataflow.DataflowRunner run
INFO: To cancel the job using the 'gcloud' tool, run:
> gcloud dataflow jobs --project=XXX cancel --region=europe-west3 {TEST_JOB_ID}
"""

APACHE_BEAM_V_2_14_0_PYTHON_SDK_LOG = f""""\
INFO:root:Completed GCS upload to gs://test-dataflow-example/staging/start-python-job-local-5bcf3d71.\
1592286375.000962/apache_beam-2.14.0-cp37-cp37m-manylinux1_x86_64.whl in 0 seconds.
INFO:root:Create job: <Job
 createTime: '2020-06-16T05:46:20.911857Z'
 currentStateTime: '1970-01-01T00:00:00Z'
 id: '{TEST_JOB_ID}'
 location: 'us-central1'
 name: 'start-python-job-local-5bcf3d71'
 projectId: 'XXX'
 stageStates: []
 startTime: '2020-06-16T05:46:20.911857Z'
 steps: []
 tempFiles: []
 type: TypeValueValuesEnum(JOB_TYPE_BATCH, 1)>
INFO:root:Created job with id: [{TEST_JOB_ID}]
INFO:root:To access the Dataflow monitoring console, please navigate to https://console.cloud.google.com/\
dataflow/jobsDetail/locations/us-central1/jobs/{TEST_JOB_ID}?project=XXX
"""

APACHE_BEAM_V_2_22_0_PYTHON_SDK_LOG = f""""\
INFO:apache_beam.runners.dataflow.internal.apiclient:Completed GCS upload to gs://test-dataflow-example/\
staging/start-python-job-local-5bcf3d71.1592286719.303624/apache_beam-2.22.0-cp37-cp37m-manylinux1_x86_64.whl\
 in 1 seconds.
INFO:apache_beam.runners.dataflow.internal.apiclient:Create job: <Job
 createTime: '2020-06-16T05:52:04.095216Z'
 currentStateTime: '1970-01-01T00:00:00Z'
 id: '{TEST_JOB_ID}'
 location: 'us-central1'
 name: 'start-python-job-local-5bcf3d71'
 projectId: 'XXX'
 stageStates: []
 startTime: '2020-06-16T05:52:04.095216Z'
 steps: []
 tempFiles: []
 type: TypeValueValuesEnum(JOB_TYPE_BATCH, 1)>
INFO:apache_beam.runners.dataflow.internal.apiclient:Created job with id: [{TEST_JOB_ID}]
INFO:apache_beam.runners.dataflow.internal.apiclient:Submitted job: {TEST_JOB_ID}
INFO:apache_beam.runners.dataflow.internal.apiclient:To access the Dataflow monitoring console, please \
navigate to https://console.cloud.google.com/dataflow/jobs/us-central1/{TEST_JOB_ID}?project=XXX
"""


class TestDataflow(unittest.TestCase):
    @parameterized.expand(
        [
            (APACHE_BEAM_V_2_14_0_JAVA_SDK_LOG,),
            (APACHE_BEAM_V_2_22_0_JAVA_SDK_LOG,),
            (APACHE_BEAM_V_2_14_0_PYTHON_SDK_LOG,),
            (APACHE_BEAM_V_2_22_0_PYTHON_SDK_LOG,),
        ],
        name_func=lambda func, num, p: f"{func.__name__}_{num}",
    )
    def test_data_flow_valid_job_id(self, log):
        echos = ";".join([f"echo {shlex.quote(line)}" for line in log.split("\n")])
        cmd = ["bash", "-c", echos]
        self.assertEqual(_DataflowRunner(cmd).wait_for_done(), TEST_JOB_ID)

    def test_data_flow_missing_job_id(self):
        cmd = ['echo', 'unit testing']
        self.assertEqual(_DataflowRunner(cmd).wait_for_done(), None)

    @mock.patch('airflow.providers.google.cloud.hooks.dataflow._DataflowRunner.log')
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
