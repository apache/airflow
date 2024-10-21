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

import os
from unittest import mock
from urllib.parse import quote_plus

import pytest
from markupsafe import escape

from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS, BashOperator
from tests_common.test_utils.db import (
    clear_db_dags,
    clear_db_runs,
    clear_rendered_ti_fields,
    initial_db_init,
)
from tests_common.test_utils.www import check_content_in_response, check_content_not_in_response

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

DEFAULT_DATE = timezone.datetime(2020, 3, 1)

pytestmark = pytest.mark.db_test


@pytest.fixture
def dag():
    return DAG(
        "testdag",
        start_date=DEFAULT_DATE,
        schedule="0 0 * * *",
        user_defined_filters={"hello": lambda name: f"Hello {name}"},
        user_defined_macros={"fullname": lambda fname, lname: f"{fname} {lname}"},
    )


@pytest.fixture
def task1(dag):
    return BashOperator(
        task_id="task1",
        bash_command="{{ task_instance_key_str }}",
        dag=dag,
    )


@pytest.fixture
def task2(dag):
    return BashOperator(
        task_id="task2",
        bash_command='echo {{ fullname("Apache", "Airflow") | hello }}',
        dag=dag,
    )


@pytest.fixture
def task3(dag):
    class TestOperator(BaseOperator):
        template_fields = ("sql",)

        def __init__(self, *, sql, **kwargs):
            super().__init__(**kwargs)
            self.sql = sql

        def execute(self, context):
            pass

    return TestOperator(
        task_id="task3",
        sql=["SELECT 1;", "SELECT 2;"],
        dag=dag,
    )


@pytest.fixture
def task4(dag):
    def func(*op_args):
        pass

    return PythonOperator(
        task_id="task4",
        python_callable=func,
        op_args=["{{ task_instance_key_str }}_args"],
        op_kwargs={"0": "{{ task_instance_key_str }}_kwargs"},
        dag=dag,
    )


@pytest.fixture
def task_secret(dag):
    return BashOperator(
        task_id="task_secret",
        bash_command="echo {{ var.value.my_secret }} && echo {{ var.value.spam }}",
        dag=dag,
    )


@pytest.fixture(scope="module", autouse=True)
def _init_blank_db():
    """Make sure there are no runs before we test anything.

    This really shouldn't be needed, but tests elsewhere leave the db dirty.
    """
    clear_db_dags()
    clear_db_runs()
    clear_rendered_ti_fields()


@pytest.fixture(autouse=True)
def _reset_db(dag, task1, task2, task3, task4, task_secret):
    yield
    clear_db_dags()
    clear_db_runs()
    clear_rendered_ti_fields()


@pytest.fixture
def create_dag_run(dag, task1, task2, task3, task4, task_secret):
    def _create_dag_run(*, execution_date, session):
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dag_run = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_date,
            data_interval=(execution_date, execution_date),
            run_type=DagRunType.SCHEDULED,
            session=session,
            **triggered_by_kwargs,
        )
        ti1 = dag_run.get_task_instance(task1.task_id, session=session)
        ti1.state = TaskInstanceState.SUCCESS
        ti2 = dag_run.get_task_instance(task2.task_id, session=session)
        ti2.state = TaskInstanceState.SCHEDULED
        ti3 = dag_run.get_task_instance(task_secret.task_id, session=session)
        ti3.state = TaskInstanceState.QUEUED
        ti4 = dag_run.get_task_instance(task3.task_id, session=session)
        ti4.state = TaskInstanceState.SUCCESS
        ti5 = dag_run.get_task_instance(task4.task_id, session=session)
        ti5.state = TaskInstanceState.SUCCESS
        session.flush()
        return dag_run

    return _create_dag_run


@pytest.fixture
def patch_app(app, dag):
    with mock.patch.object(app, "dag_bag") as mock_dag_bag:
        mock_dag_bag.get_dag.return_value = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        yield app


@pytest.mark.usefixtures("patch_app")
def test_rendered_template_view(admin_client, create_dag_run, task1):
    """
    Test that the Rendered View contains the values from RenderedTaskInstanceFields
    """
    assert task1.bash_command == "{{ task_instance_key_str }}"

    with create_session() as session:
        dag_run = create_dag_run(execution_date=DEFAULT_DATE, session=session)
        ti = dag_run.get_task_instance(task1.task_id, session=session)
        assert ti is not None, "task instance not found"
        ti.refresh_from_task(task1)
        session.add(RenderedTaskInstanceFields(ti))

    url = f"rendered-templates?task_id=task1&dag_id=testdag&execution_date={quote_plus(str(DEFAULT_DATE))}"

    resp = admin_client.get(url, follow_redirects=True)
    check_content_in_response("testdag__task1__20200301", resp)


@pytest.mark.usefixtures("patch_app")
def test_rendered_template_view_for_unexecuted_tis(admin_client, create_dag_run, task1):
    """
    Test that the Rendered View is able to show rendered values
    even for TIs that have not yet executed
    """
    assert task1.bash_command == "{{ task_instance_key_str }}"

    with create_session() as session:
        create_dag_run(execution_date=DEFAULT_DATE, session=session)

    url = f"rendered-templates?task_id=task1&dag_id=testdag&execution_date={quote_plus(str(DEFAULT_DATE))}"

    resp = admin_client.get(url, follow_redirects=True)
    check_content_in_response("testdag__task1__20200301", resp)


@pytest.mark.usefixtures("patch_app")
def test_user_defined_filter_and_macros_raise_error(admin_client, create_dag_run, task2):
    assert task2.bash_command == 'echo {{ fullname("Apache", "Airflow") | hello }}'

    with create_session() as session:
        create_dag_run(execution_date=DEFAULT_DATE, session=session)

    url = f"rendered-templates?task_id=task2&dag_id=testdag&execution_date={quote_plus(str(DEFAULT_DATE))}"

    resp = admin_client.get(url, follow_redirects=True)
    assert resp.status_code == 200

    resp_html: str = resp.data.decode("utf-8")
    assert "echo Hello Apache Airflow" not in resp_html
    assert (
        "Webserver does not have access to User-defined Macros or Filters when "
        "Dag Serialization is enabled. Hence for the task that have not yet "
        "started running, please use &#39;airflow tasks render&#39; for "
        "debugging the rendering of template_fields.<br><br>"
    ) in resp_html

    # MarkupSafe changed the exception detail from 'no filter named' to
    # 'No filter named' in 2.0 (I think), so we normalize for comparison.
    assert "originalerror: no filter named &#39;hello&#39;" in resp_html.lower()


@pytest.mark.enable_redact
@pytest.mark.usefixtures("patch_app")
def test_rendered_template_secret(admin_client, create_dag_run, task_secret):
    """Test that the Rendered View masks values retrieved from secret variables."""
    Variable.set("my_secret", "secret_unlikely_to_happen_accidentally")
    Variable.set("spam", "egg")

    assert task_secret.bash_command == "echo {{ var.value.my_secret }} && echo {{ var.value.spam }}"

    with create_session() as session:
        dag_run = create_dag_run(execution_date=DEFAULT_DATE, session=session)
        ti = dag_run.get_task_instance(task_secret.task_id, session=session)
        assert ti is not None, "task instance not found"
        ti.refresh_from_task(task_secret)
        assert ti.state == TaskInstanceState.QUEUED

    date = quote_plus(str(DEFAULT_DATE))
    url = f"rendered-templates?task_id=task_secret&dag_id=testdag&execution_date={date}"

    resp = admin_client.get(url, follow_redirects=True)
    check_content_in_response("***", resp)
    check_content_not_in_response("secret_unlikely_to_happen_accidentally", resp)
    ti.refresh_from_task(task_secret)
    assert ti.state == TaskInstanceState.QUEUED


if os.environ.get("_AIRFLOW_SKIP_DB_TESTS") == "true":
    # Handle collection of the test by non-db case
    Variable = mock.MagicMock()  # type: ignore[misc]
else:
    initial_db_init()


@pytest.mark.enable_redact
@pytest.mark.parametrize(
    "env, expected",
    [
        pytest.param(
            {"plain_key": "plain_value"},
            "{'plain_key': 'plain_value'}",
            id="env-plain-key-val",
        ),
        pytest.param(
            {"plain_key": Variable.setdefault("plain_var", "banana")},
            "{'plain_key': 'banana'}",
            id="env-plain-key-plain-var",
        ),
        pytest.param(
            {"plain_key": Variable.setdefault("secret_var", "monkey")},
            "{'plain_key': '***'}",
            id="env-plain-key-sensitive-var",
        ),
        pytest.param(
            {"plain_key": "{{ var.value.plain_var }}"},
            "{'plain_key': '{{ var.value.plain_var }}'}",
            id="env-plain-key-plain-tpld-var",
        ),
        pytest.param(
            {"plain_key": "{{ var.value.secret_var }}"},
            "{'plain_key': '{{ var.value.secret_var }}'}",
            id="env-plain-key-sensitive-tpld-var",
        ),
        pytest.param(
            {"secret_key": "plain_value"},
            "{'secret_key': '***'}",
            id="env-sensitive-key-plain-val",
        ),
        pytest.param(
            {"secret_key": Variable.setdefault("plain_var", "monkey")},
            "{'secret_key': '***'}",
            id="env-sensitive-key-plain-var",
        ),
        pytest.param(
            {"secret_key": Variable.setdefault("secret_var", "monkey")},
            "{'secret_key': '***'}",
            id="env-sensitive-key-sensitive-var",
        ),
        pytest.param(
            {"secret_key": "{{ var.value.plain_var }}"},
            "{'secret_key': '***'}",
            id="env-sensitive-key-plain-tpld-var",
        ),
        pytest.param(
            {"secret_key": "{{ var.value.secret_var }}"},
            "{'secret_key': '***'}",
            id="env-sensitive-key-sensitive-tpld-var",
        ),
    ],
)
def test_rendered_task_detail_env_secret(patch_app, admin_client, request, env, expected):
    if request.node.callspec.id.endswith("-tpld-var"):
        Variable.set("plain_var", "banana")
        Variable.set("secret_var", "monkey")

    dag: DAG = patch_app.dag_bag.get_dag("testdag")
    task_secret: BashOperator = dag.get_task(task_id="task1")
    task_secret.env = env
    date = quote_plus(str(DEFAULT_DATE))
    url = f"task?task_id=task1&dag_id=testdag&execution_date={date}"

    with create_session() as session:
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            run_type=DagRunType.SCHEDULED,
            session=session,
            **triggered_by_kwargs,
        )

    resp = admin_client.get(url, follow_redirects=True)
    check_content_in_response(str(escape(expected)), resp)

    if request.node.callspec.id.endswith("-tpld-var"):
        Variable.delete("plain_var")
        Variable.delete("secret_var")


@pytest.mark.usefixtures("patch_app")
def test_rendered_template_view_for_list_template_field_args(admin_client, create_dag_run, task3):
    """
    Test that the Rendered View can show a list of syntax-highlighted SQL statements
    """
    assert task3.sql == ["SELECT 1;", "SELECT 2;"]

    with create_session() as session:
        create_dag_run(execution_date=DEFAULT_DATE, session=session)

    url = f"rendered-templates?task_id=task3&dag_id=testdag&execution_date={quote_plus(str(DEFAULT_DATE))}"

    resp = admin_client.get(url, follow_redirects=True)
    check_content_in_response("List item #0", resp)
    check_content_in_response("List item #1", resp)


@pytest.mark.usefixtures("patch_app")
def test_rendered_template_view_for_op_args(admin_client, create_dag_run, task4):
    """
    Test that the Rendered View can show rendered values in op_args and op_kwargs
    """
    assert task4.op_args == ["{{ task_instance_key_str }}_args"]
    assert list(task4.op_kwargs.values()) == ["{{ task_instance_key_str }}_kwargs"]

    with create_session() as session:
        create_dag_run(execution_date=DEFAULT_DATE, session=session)

    url = f"rendered-templates?task_id=task4&dag_id=testdag&execution_date={quote_plus(str(DEFAULT_DATE))}"

    resp = admin_client.get(url, follow_redirects=True)
    check_content_in_response("testdag__task4__20200301_args", resp)
    check_content_in_response("testdag__task4__20200301_kwargs", resp)
