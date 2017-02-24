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
import mock

from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator


TASK_ID = 'test-python-dataflow'
PY_FILE = 'gs://my-bucket/my-object.py'
PY_OPTIONS = ['-m']
DEFAULT_OPTIONS = {
    'project': 'test',
    'stagingLocation': 'gs://test/staging'
}
ADDITIONAL_OPTIONS = {
    'output': 'gs://test/output'
}


class DataFlowPythonOperatorTest(unittest.TestCase):

    def setUp(self):
        self.dataflow = DataFlowPythonOperator(
            task_id=TASK_ID,
            py_file=PY_FILE,
            py_options=PY_OPTIONS,
            dataflow_default_options=DEFAULT_OPTIONS,
            options=ADDITIONAL_OPTIONS)

    def test_init(self):
        """Test DataFlowPythonOperator instance is properly initialized."""
        self.assertEqual(self.dataflow.task_id, TASK_ID)
        self.assertEqual(self.dataflow.py_file, PY_FILE)
        self.assertEqual(self.dataflow.py_options, PY_OPTIONS)
        self.assertEqual(self.dataflow.dataflow_default_options,
                         DEFAULT_OPTIONS)
        self.assertEqual(self.dataflow.options,
                         ADDITIONAL_OPTIONS)

    @mock.patch('airflow.contrib.operators.dataflow_operator.DataFlowHook')
    @mock.patch('airflow.contrib.operators.dataflow_operator.GoogleCloudStorageHook')
    def test_exec(self, mock_gcs_hook, mock_dataflow_hook):
        """Test DataFlowHook is created and the right args are passed to
        start_python_workflow.

        """
        mock_gcs_download = mock.Mock(return_value=42)
        mock_gcs_hook.return_value.download = mock_gcs_download
        mock_start_python = mock_dataflow_hook.return_value.start_python_dataflow

        self.dataflow.execute(None)
        self.assertTrue(mock_gcs_hook.called)
        expected_options = {
            'project': 'test',
            'staging_location': 'gs://test/staging',
            'output': 'gs://test/output'
        }
        mock_gcs_download.assert_called_once_with('my-bucket', 'my-object.py',
                                                  mock.ANY)
        mock_start_python.assert_called_once_with(TASK_ID, expected_options,
                                                  mock.ANY, PY_OPTIONS)
        self.assertTrue(self.dataflow.py_file.startswith('/tmp/dataflow'))
