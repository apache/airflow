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

from airflow.models import Connection
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.hooks.gcp_dataflow_hook import DataFlowHook

TASK_ID = 'test-python-dataflow'
PY_FILE = 'apache_beam.examples.wordcount'
PY_OPTIONS = ['-m']
OPTIONS = {
    'project': 'test',
    'staging_location': 'gs://test/staging'
}


class DataFlowHookTest(unittest.TestCase):

    @mock.patch('airflow.contrib.hooks.gcp_dataflow_hook.DataFlowHook._start_dataflow')
    @mock.patch.object(GoogleCloudBaseHook, 'get_connection', Connection)
    def test_start_python_dataflow(self, internal_dataflow_mock):
        dataflow_hook = DataFlowHook(gcp_conn_id='test')
        dataflow_hook.start_python_dataflow(
            task_id=TASK_ID, variables=OPTIONS,
            dataflow=PY_FILE, py_options=PY_OPTIONS)
        internal_dataflow_mock.assert_called_once_with(
            TASK_ID, OPTIONS, PY_FILE, mock.ANY, ['python'] + PY_OPTIONS)
