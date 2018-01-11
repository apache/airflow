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
#

import unittest
from mock import call
from mock import MagicMock

from airflow.contrib.hooks.gcp_dataflow_hook import DataFlowHook
from airflow.contrib.hooks.gcp_dataflow_hook import _Dataflow

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


TASK_ID = 'test-dataflow-operator'
TEMPLATE = 'gs://dataflow-templates/wordcount/template_file'
PARAMETERS = {
    'inputFile': 'gs://dataflow-samples/shakespeare/kinglear.txt',
    'output': 'gs://test/output/my_output'
}
PY_FILE = 'apache_beam.examples.wordcount'
JAR_FILE = 'unitest.jar'
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
BASE_STRING = 'airflow.contrib.hooks.gcp_api_base_hook.{}'
DATAFLOW_STRING = 'airflow.contrib.hooks.gcp_dataflow_hook.{}'
MOCK_UUID = '12345678'


def mock_init(self, gcp_conn_id, delegate_to=None):
    pass


class DataFlowPythonHookTest(unittest.TestCase):

    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleCloudBaseHook.__init__'),
                        new=mock_init):
            self.dataflow_hook = DataFlowHook(gcp_conn_id='test')

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid1'))
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
            task_id=TASK_ID, variables=DATAFLOW_OPTIONS_PY,
            dataflow=PY_FILE, py_options=PY_OPTIONS)
        EXPECTED_CMD = ['python', '-m', PY_FILE,
                        '--runner=DataflowRunner', '--project=test',
                        '--labels=foo=bar',
                        '--staging_location=gs://test/staging',
                        '--job_name={}-{}'.format(TASK_ID, MOCK_UUID)]
        self.assertListEqual(sorted(mock_dataflow.call_args[0][0]),
                             sorted(EXPECTED_CMD))

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid1'))
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
            task_id=TASK_ID, variables=DATAFLOW_OPTIONS_JAVA,
            dataflow=JAR_FILE)
        EXPECTED_CMD = ['java', '-jar', JAR_FILE,
                        '--runner=DataflowRunner', '--project=test',
                        '--stagingLocation=gs://test/staging',
                        '--labels={"foo":"bar"}',
                        '--jobName={}-{}'.format(TASK_ID, MOCK_UUID)]
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
      mock_proc.stderr.readlines = MagicMock(return_value=['test\n','error\n'])
      mock_stderr_fd = MagicMock()
      mock_proc.stderr.fileno = MagicMock(return_value=mock_stderr_fd)
      mock_proc_poll = MagicMock()
      mock_select.return_value = [[mock_stderr_fd]]
      def poll_resp_error():
        mock_proc.return_code = 1
        return True
      mock_proc_poll.side_effect=[None, poll_resp_error]
      mock_proc.poll = mock_proc_poll
      mock_popen.return_value = mock_proc
      dataflow = _Dataflow(['test', 'cmd'])
      mock_logging.info.assert_called_with('Running command: %s', 'test cmd')
      self.assertRaises(Exception, dataflow.wait_for_done)
      mock_logging.warning.assert_has_calls([call('test'), call('error')])


class DataFlowTemplateHookTest(unittest.TestCase):

    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleCloudBaseHook.__init__'),
                        new=mock_init):
            self.dataflow_hook = DataFlowHook(gcp_conn_id='test')

    @mock.patch(DATAFLOW_STRING.format('DataFlowHook._start_template_dataflow'))
    def test_start_template_dataflow(self, internal_dataflow_mock):
        self.dataflow_hook.start_template_dataflow(
            task_id=TASK_ID, variables=DATAFLOW_OPTIONS_TEMPLATE, parameters=PARAMETERS,
            dataflow_template=TEMPLATE)
        internal_dataflow_mock.assert_called_once_with(
            mock.ANY, DATAFLOW_OPTIONS_TEMPLATE, PARAMETERS, TEMPLATE)
