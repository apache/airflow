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
from __future__ import annotations

from unittest import TestCase, mock

from qds_sdk.commands import PrestoCommand

from airflow.providers.qubole.hooks.qubole import QuboleHook

DAG_ID = "qubole_test_dag"
TASK_ID = "test_task"
RESULTS_WITH_HEADER = 'header1\theader2\nval1\tval2'
RESULTS_WITH_NO_HEADER = 'val1\tval2'

add_tags = QuboleHook._add_tags


def get_result_mock(fp, inline, delim, fetch, arguments):
    if arguments[0] == 'true':
        fp.write(bytearray(RESULTS_WITH_HEADER, 'utf-8'))
    else:
        fp.write(bytearray(RESULTS_WITH_NO_HEADER, 'utf-8'))


class TestQuboleHook(TestCase):
    def test_add_string_to_tags(self):
        tags = {'dag_id', 'task_id'}
        add_tags(tags, 'string')
        assert {'dag_id', 'task_id', 'string'} == tags

    def test_add_list_to_tags(self):
        tags = {'dag_id', 'task_id'}
        add_tags(tags, ['value1', 'value2'])
        assert {'dag_id', 'task_id', 'value1', 'value2'} == tags

    def test_add_tuple_to_tags(self):
        tags = {'dag_id', 'task_id'}
        add_tags(tags, ('value1', 'value2'))
        assert {'dag_id', 'task_id', 'value1', 'value2'} == tags

    @mock.patch('qds_sdk.commands.Command.get_results', new=get_result_mock)
    def test_get_results_with_headers(self):
        dag = mock.MagicMock()
        dag.dag_id = DAG_ID
        hook = QuboleHook(task_id=TASK_ID, command_type='prestocmd', dag=dag)

        task = mock.MagicMock()
        task.xcom_pull.return_value = 'test_command_id'
        with mock.patch('qds_sdk.resource.Resource.find', return_value=PrestoCommand):
            results = open(hook.get_results(ti=task, include_headers=True)).read()
            assert results == RESULTS_WITH_HEADER

    @mock.patch('qds_sdk.commands.Command.get_results', new=get_result_mock)
    def test_get_results_without_headers(self):
        dag = mock.MagicMock()
        dag.dag_id = DAG_ID
        hook = QuboleHook(task_id=TASK_ID, command_type='prestocmd', dag=dag)

        task = mock.MagicMock()
        task.xcom_pull.return_value = 'test_command_id'

        with mock.patch('qds_sdk.resource.Resource.find', return_value=PrestoCommand):
            results = open(hook.get_results(ti=task, include_headers=False)).read()
            assert results == RESULTS_WITH_NO_HEADER
