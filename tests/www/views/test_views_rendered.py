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
from unittest import mock
from urllib.parse import quote_plus

import pytest

from airflow.models import DAG, RenderedTaskInstanceFields, TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from airflow.utils.session import create_session

DEFAULT_DATE = timezone.datetime(2020, 3, 1)


@pytest.fixture()
def dag():
    return DAG(
        "testdag",
        start_date=DEFAULT_DATE,
        user_defined_filters={"hello": lambda name: f'Hello {name}'},
        user_defined_macros={"fullname": lambda fname, lname: f'{fname} {lname}'},
    )


@pytest.fixture(autouse=True)
def task1(dag):
    return BashOperator(
        task_id='task1',
        bash_command='{{ task_instance_key_str }}',
        dag=dag,
    )


@pytest.fixture(autouse=True)
def task2(dag):
    return BashOperator(
        task_id='task2',
        bash_command='echo {{ fullname("Apache", "Airflow") | hello }}',
        dag=dag,
    )


@pytest.fixture(autouse=True)
def reset_db(dag):
    with create_session() as session:
        SerializedDagModel.write_dag(dag)
        session.query(RenderedTaskInstanceFields).delete()
    yield
    with create_session() as session:
        session.query(RenderedTaskInstanceFields).delete()
        session.query(SerializedDagModel).delete()


@pytest.fixture()
def patch_app(app, dag):
    with mock.patch.object(app, "dag_bag") as mock_dag_bag:
        mock_dag_bag.get_dag.return_value = dag
        yield app


@pytest.mark.usefixtures("patch_app")
def test_rendered_template_view(admin_client, checker, task1):
    """
    Test that the Rendered View contains the values from RenderedTaskInstanceFields
    """
    assert task1.bash_command == '{{ task_instance_key_str }}'
    ti = TaskInstance(task1, DEFAULT_DATE)

    with create_session() as session:
        session.add(RenderedTaskInstanceFields(ti))

    url = f'rendered-templates?task_id=task1&dag_id=testdag&execution_date={quote_plus(str(DEFAULT_DATE))}'

    resp = admin_client.get(url, follow_redirects=True)
    checker.check_content_in_response("testdag__task1__20200301", resp)


@pytest.mark.usefixtures("patch_app")
def test_rendered_template_view_for_unexecuted_tis(admin_client, checker, task1):
    """
    Test that the Rendered View is able to show rendered values
    even for TIs that have not yet executed
    """
    assert task1.bash_command == '{{ task_instance_key_str }}'

    url = f'rendered-templates?task_id=task1&dag_id=task1&execution_date={quote_plus(str(DEFAULT_DATE))}'

    resp = admin_client.get(url, follow_redirects=True)
    checker.check_content_in_response("testdag__task1__20200301", resp)


def test_user_defined_filter_and_macros_raise_error(app, admin_client, checker, dag, task2):
    """
    Test that the Rendered View is able to show rendered values
    even for TIs that have not yet executed
    """
    dag = SerializedDagModel.get(dag.dag_id).dag
    with mock.patch.object(app, "dag_bag") as mock_dag_bag:
        mock_dag_bag.get_dag.return_value = dag

        assert task2.bash_command == 'echo {{ fullname("Apache", "Airflow") | hello }}'

        url = (
            f'rendered-templates?task_id=task2&dag_id=testdag&'
            f'execution_date={quote_plus(str(DEFAULT_DATE))}'
        )

        resp = admin_client.get(url, follow_redirects=True)

        checker.check_content_not_in_response("echo Hello Apache Airflow", resp)
        checker.check_content_in_response(
            "Webserver does not have access to User-defined Macros or Filters when "
            "Dag Serialization is enabled. Hence for the task that have not yet "
            "started running, please use &#39;airflow tasks render&#39; for "
            "debugging the rendering of template_fields.<br><br>OriginalError: no "
            "filter named &#39;hello&#39",
            resp,
        )
