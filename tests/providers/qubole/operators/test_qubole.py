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

from unittest import mock

import pytest

from airflow import settings
from airflow.models import DAG, Connection
from airflow.providers.qubole.hooks.qubole import QuboleHook
from airflow.providers.qubole.operators.qubole import QDSLink, QuboleOperator
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils import db
from airflow.utils.timezone import datetime

DAG_ID = "qubole_test_dag"
TASK_ID = "test_task"
DEFAULT_CONN = "qubole_default"
TEMPLATE_CONN = "my_conn_id"
TEST_CONN = "qubole_test_conn"
DEFAULT_DATE = datetime(2017, 1, 1)


class TestQuboleOperator:
    def setup_method(self):
        db.merge_conn(Connection(conn_id=DEFAULT_CONN, conn_type="HTTP"))
        db.merge_conn(Connection(conn_id=TEST_CONN, conn_type="HTTP", host="http://localhost/api"))

    def teardown_method(self):
        session = settings.Session()
        session.query(Connection).filter(Connection.conn_id == TEST_CONN).delete()
        session.flush()
        session.close()

    def test_init_with_default_connection(self):
        op = QuboleOperator(task_id=TASK_ID)
        assert op.task_id == TASK_ID
        assert op.qubole_conn_id == DEFAULT_CONN

    def test_init_with_template_connection(self):
        with DAG(DAG_ID, start_date=DEFAULT_DATE):
            task = QuboleOperator(task_id=TASK_ID, qubole_conn_id="{{ qubole_conn_id }}")

        task.render_template_fields({"qubole_conn_id": TEMPLATE_CONN})
        assert task.task_id == TASK_ID
        assert task.qubole_conn_id == TEMPLATE_CONN

    def test_init_with_template_cluster_label(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            QuboleOperator,
            dag_id="test_init_with_template_cluster_label",
            execution_date=DEFAULT_DATE,
            task_id=TASK_ID,
            cluster_label="{{ params.cluster_label }}",
            params={"cluster_label": "default"},
        )
        ti.render_templates()
        assert ti.task.cluster_label == "default"

    def test_get_hook(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        with dag:
            task = QuboleOperator(task_id=TASK_ID, command_type="hivecmd", dag=dag)

        hook = task.get_hook()
        assert hook.__class__ == QuboleHook

    def test_hyphen_args_note_id(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        with dag:
            task = QuboleOperator(task_id=TASK_ID, command_type="sparkcmd", note_id="123", dag=dag)

        assert task.get_hook().create_cmd_args({"run_id": "dummy"})[0] == "--note-id=123"

    def test_notify(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        with dag:
            task = QuboleOperator(task_id=TASK_ID, command_type="sparkcmd", notify=True, dag=dag)

        assert task.get_hook().create_cmd_args({"run_id": "dummy"})[0] == "--notify"

    def test_position_args_parameters(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        with dag:
            task = QuboleOperator(
                task_id=TASK_ID, command_type="pigcmd", parameters="key1=value1 key2=value2", dag=dag
            )

        assert task.get_hook().create_cmd_args({"run_id": "dummy"})[1] == "key1=value1"
        assert task.get_hook().create_cmd_args({"run_id": "dummy"})[2] == "key2=value2"

        cmd = "s3distcp --src s3n://airflow/source_hadoopcmd --dest s3n://airflow/destination_hadoopcmd"
        task = QuboleOperator(task_id=TASK_ID + "_1", command_type="hadoopcmd", dag=dag, sub_command=cmd)

        assert task.get_hook().create_cmd_args({"run_id": "dummy"})[1] == "s3distcp"
        assert task.get_hook().create_cmd_args({"run_id": "dummy"})[2] == "--src"
        assert task.get_hook().create_cmd_args({"run_id": "dummy"})[3] == "s3n://airflow/source_hadoopcmd"
        assert task.get_hook().create_cmd_args({"run_id": "dummy"})[4] == "--dest"
        assert (
            task.get_hook().create_cmd_args({"run_id": "dummy"})[5] == "s3n://airflow/destination_hadoopcmd"
        )

    def test_get_link(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            QuboleOperator,
            dag_id="test_get_link",
            execution_date=DEFAULT_DATE,
            task_id=TASK_ID,
            qubole_conn_id=TEST_CONN,
            command_type="shellcmd",
            parameters="param1 param2",
        )
        ti.xcom_push("qbol_cmd_id", 12345)

        url = ti.task.get_extra_links(ti, "Go to QDS")
        assert url == "http://localhost/v2/analyze?command_id=12345"

    @pytest.mark.need_serialized_dag
    def test_extra_serialized_field(self, dag_maker, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            QuboleOperator,
            dag_id="test_extra_serialized_field",
            execution_date=DEFAULT_DATE,
            task_id=TASK_ID,
            command_type="shellcmd",
            qubole_conn_id=TEST_CONN,
        )

        serialized_dag = dag_maker.get_serialized_data()
        assert "qubole_conn_id" in serialized_dag["dag"]["tasks"][0]

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict[TASK_ID]
        assert getattr(simple_task, "qubole_conn_id") == TEST_CONN

        assert isinstance(list(simple_task.operator_extra_links)[0], QDSLink)

        ti.xcom_push("qbol_cmd_id", 12345)
        url = simple_task.get_extra_links(ti, "Go to QDS")
        assert url == "http://localhost/v2/analyze?command_id=12345"

    def test_parameter_pool_passed(self):
        test_pool = "test_pool"
        op = QuboleOperator(task_id=TASK_ID, pool=test_pool)
        assert op.pool == test_pool

    @mock.patch("airflow.providers.qubole.hooks.qubole.QuboleHook.get_results")
    def test_parameter_include_header_passed(self, mock_get_results):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)
        qubole_operator = QuboleOperator(task_id=TASK_ID, dag=dag, command_type="prestocmd")
        qubole_operator.get_results(include_headers=True)
        mock_get_results.asset_called_with("include_headers", True)

    @mock.patch("airflow.providers.qubole.hooks.qubole.QuboleHook.get_results")
    def test_parameter_include_header_missing(self, mock_get_results):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)
        qubole_operator = QuboleOperator(task_id=TASK_ID, dag=dag, command_type="prestocmd")
        qubole_operator.get_results()
        mock_get_results.asset_called_with("include_headers", False)
