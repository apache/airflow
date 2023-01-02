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
from urllib.parse import quote_plus

import pytest

from airflow.models import DAG, RenderedTaskInstanceFields, Variable
from airflow.operators.bash import BashOperator
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_rendered_ti_fields
from tests.test_utils.www import check_content_in_response, check_content_not_in_response

DEFAULT_DATE = timezone.datetime(2020, 3, 1)


@pytest.fixture()
def dag():
    return DAG(
        "testdag",
        start_date=DEFAULT_DATE,
        user_defined_filters={"hello": lambda name: f"Hello {name}"},
        user_defined_macros={"fullname": lambda fname, lname: f"{fname} {lname}"},
    )


@pytest.fixture()
def task1(dag):
    return BashOperator(
        task_id="task1",
        bash_command="{{ task_instance_key_str }}",
        dag=dag,
    )


@pytest.fixture()
def task2(dag):
    return BashOperator(
        task_id="task2",
        bash_command='echo {{ fullname("Apache", "Airflow") | hello }}',
        dag=dag,
    )


@pytest.fixture()
def task_secret(dag):
    return BashOperator(
        task_id="task_secret",
        bash_command="echo {{ var.value.my_secret }} && echo {{ var.value.spam }}",
        dag=dag,
    )


@pytest.fixture(scope="module", autouse=True)
def init_blank_db():
    """Make sure there are no runs before we test anything.

    This really shouldn't be needed, but tests elsewhere leave the db dirty.
    """
    clear_db_dags()
    clear_db_runs()
    clear_rendered_ti_fields()


@pytest.fixture(autouse=True)
def reset_db(dag, task1, task2, task_secret):
    yield
    clear_db_dags()
    clear_db_runs()
    clear_rendered_ti_fields()


@pytest.fixture()
def create_dag_run(dag, task1, task2, task_secret):
    def _create_dag_run(*, execution_date, session):
        dag_run = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_date,
            data_interval=(execution_date, execution_date),
            run_type=DagRunType.SCHEDULED,
            session=session,
        )
        ti1 = dag_run.get_task_instance(task1.task_id, session=session)
        ti1.state = TaskInstanceState.SUCCESS
        ti2 = dag_run.get_task_instance(task2.task_id, session=session)
        ti2.state = TaskInstanceState.SCHEDULED
        ti3 = dag_run.get_task_instance(task_secret.task_id, session=session)
        ti3.state = TaskInstanceState.QUEUED
        session.flush()
        return dag_run

    return _create_dag_run


@pytest.fixture()
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
