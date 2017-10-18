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


TASK_ID = 'test-python-dataflow'
PY_FILE = 'apache_beam.examples.wordcount'
PY_OPTIONS = ['-m']
OPTIONS = {
    'project': 'test',
    'staging_location': 'gs://test/staging'
}
BASE_STRING = 'airflow.contrib.hooks.gcp_api_base_hook.{}'
DATAFLOW_STRING = 'airflow.contrib.hooks.gcp_dataflow_hook.{}'


def mock_init(self, gcp_conn_id, delegate_to=None):
    pass


class DataFlowHookTest(unittest.TestCase):

    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleCloudBaseHook.__init__'),
                        new=mock_init):
            self.dataflow_hook = DataFlowHook(gcp_conn_id='test')

    @mock.patch(DATAFLOW_STRING.format('DataFlowHook._start_dataflow'))
    def test_start_python_dataflow(self, internal_dataflow_mock):
        self.dataflow_hook.start_python_dataflow(
            task_id=TASK_ID, variables=OPTIONS,
            dataflow=PY_FILE, py_options=PY_OPTIONS)
        internal_dataflow_mock.assert_called_once_with(
            TASK_ID, OPTIONS, PY_FILE, mock.ANY, ['python'] + PY_OPTIONS)

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

