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
from airflow.contrib.hooks.gcp_dataflow_hook import DataFlowHook

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
