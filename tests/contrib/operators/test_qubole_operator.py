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
from datetime import datetime

from airflow.models import DAG, Connection
from airflow.utils import db

from airflow.contrib.hooks.qubole_hook import QuboleHook
from airflow.contrib.operators.qubole_operator import QuboleOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

DAG_ID="qubole_test_dag"
TASK_ID="test_task"
DEFAULT_CONN="qubole_default"
TEMPLATE_CONN = "my_conn_id"
DEFAULT_DATE = datetime(2017, 1, 1)


class QuboleOperatorTest(unittest.TestCase):
    def setUp(self):
        db.merge_conn(
            Connection(conn_id=DEFAULT_CONN, conn_type='HTTP'))

    def test_init_with_default_connection(self):
        op = QuboleOperator(task_id=TASK_ID)
        self.assertEqual(op.task_id, TASK_ID)
        self.assertEqual(op.qubole_conn_id, DEFAULT_CONN)

    def test_init_with_template_connection(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        with dag:
            task = QuboleOperator(task_id=TASK_ID, dag=dag,
                                  qubole_conn_id="{{ dag_run.conf['qubole_conn_id'] }}")

        result = task.render_template('qubole_conn_id', "{{ qubole_conn_id }}",
                                      {'qubole_conn_id' : TEMPLATE_CONN})
        self.assertEqual(task.task_id, TASK_ID)
        self.assertEqual(result, TEMPLATE_CONN)

    def test_get_hook(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        with dag:
            task = QuboleOperator(task_id=TASK_ID, command_type='hivecmd', dag=dag)

        hook = task.get_hook()
        self.assertEqual(hook.__class__, QuboleHook)

    def test_hyphen_args_note_id(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        with dag:
            task = QuboleOperator(task_id=TASK_ID, command_type='sparkcmd',
                                  note_id="123", dag=dag)
        self.assertEqual(task.get_hook().create_cmd_args({'run_id':'dummy'})[0],
                         "--note-id=123")

    def test_position_args_parameters(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        with dag:
            task = QuboleOperator(task_id=TASK_ID, command_type='pigcmd',
                          parameters="key1=value1 key2=value2", dag=dag)

        self.assertEqual(task.get_hook().create_cmd_args({'run_id':'dummy'})[1],
                         "key1=value1")
        self.assertEqual(task.get_hook().create_cmd_args({'run_id':'dummy'})[2],
                         "key2=value2")




