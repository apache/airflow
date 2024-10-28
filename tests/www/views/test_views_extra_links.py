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

import json
import urllib.parse
from unittest import mock

import pytest

from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.utils import timezone
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS, BaseOperatorLink
from tests_common.test_utils.db import clear_db_runs
from tests_common.test_utils.mock_operators import (
    AirflowLink,
    EmptyExtraLinkTestOperator,
    EmptyNoExtraLinkTestOperator,
)

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2017, 1, 1, tzinfo=timezone.utc)
STR_DEFAULT_DATE = urllib.parse.quote(DEFAULT_DATE.strftime("%Y-%m-%dT%H:%M:%S.%f%z"))

ENDPOINT = "extra_links"


class RaiseErrorLink(BaseOperatorLink):
    name = "raise_error"

    def get_link(self, operator, *, ti_key):
        raise ValueError("This is an error")


class NoResponseLink(BaseOperatorLink):
    name = "no_response"

    def get_link(self, operator, *, ti_key):
        return None


class FooBarLink(BaseOperatorLink):
    name = "foo-bar"

    def get_link(self, operator, *, ti_key):
        return f"http://www.example.com/{operator.task_id}/foo-bar/{ti_key.run_id}"


class DummyTestOperator(BaseOperator):
    # We need to ignore type check here due to 2.7 compatibility import
    operator_extra_links = (  # type: ignore[assignment]
        RaiseErrorLink(),
        NoResponseLink(),
        FooBarLink(),
        AirflowLink(),
    )


@pytest.fixture(scope="module")
def dag():
    return DAG("dag", start_date=DEFAULT_DATE, schedule="0 0 * * *")


@pytest.fixture(scope="module")
def create_dag_run(dag):
    def _create_dag_run(*, execution_date, session):
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        return dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_date,
            data_interval=(execution_date, execution_date),
            run_type=DagRunType.MANUAL,
            session=session,
            **triggered_by_kwargs,
        )

    return _create_dag_run


@pytest.fixture
def dag_run(create_dag_run, session):
    return create_dag_run(execution_date=DEFAULT_DATE, session=session)


@pytest.fixture(scope="module", autouse=True)
def _patched_app(app, dag):
    with mock.patch.object(app, "dag_bag") as mock_dag_bag:
        mock_dag_bag.get_dag.return_value = dag
        yield


@pytest.fixture(scope="module", autouse=True)
def task_1(dag):
    return DummyTestOperator(task_id="some_dummy_task", dag=dag)


@pytest.fixture(scope="module", autouse=True)
def task_2(dag):
    return EmptyExtraLinkTestOperator(task_id="some_dummy_task_2", dag=dag)


@pytest.fixture(scope="module", autouse=True)
def task_3(dag):
    return EmptyNoExtraLinkTestOperator(task_id="some_dummy_task_3", dag=dag)


@pytest.fixture(scope="module", autouse=True)
def _init_blank_task_instances():
    """Make sure there are no runs before we test anything.

    This really shouldn't be needed, but tests elsewhere leave the db dirty.
    """
    clear_db_runs()


@pytest.fixture(autouse=True)
def _reset_task_instances():
    yield
    clear_db_runs()


def test_extra_links_works(dag_run, task_1, viewer_client, session):
    response = viewer_client.get(
        f"{ENDPOINT}?dag_id={task_1.dag_id}&task_id={task_1.task_id}"
        f"&execution_date={STR_DEFAULT_DATE}&link_name=foo-bar",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert json.loads(response.data.decode()) == {
        "url": "http://www.example.com/some_dummy_task/foo-bar/manual__2017-01-01T00:00:00+00:00",
        "error": None,
    }


def test_global_extra_links_works(dag_run, task_1, viewer_client, session):
    response = viewer_client.get(
        f"{ENDPOINT}?dag_id={dag_run.dag_id}&task_id={task_1.task_id}"
        f"&execution_date={STR_DEFAULT_DATE}&link_name=github",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert json.loads(response.data.decode()) == {
        "url": "https://github.com/apache/airflow",
        "error": None,
    }


def test_operator_extra_link_override_global_extra_link(dag_run, task_1, viewer_client):
    response = viewer_client.get(
        f"{ENDPOINT}?dag_id={task_1.dag_id}&task_id={task_1.task_id}"
        f"&execution_date={STR_DEFAULT_DATE}&link_name=airflow",
        follow_redirects=True,
    )

    assert response.status_code == 200
    response_str = response.data
    if isinstance(response.data, bytes):
        response_str = response_str.decode()
    assert json.loads(response_str) == {"url": "https://airflow.apache.org", "error": None}


def test_extra_links_error_raised(dag_run, task_1, viewer_client):
    response = viewer_client.get(
        f"{ENDPOINT}?dag_id={task_1.dag_id}&task_id={task_1.task_id}"
        f"&execution_date={STR_DEFAULT_DATE}&link_name=raise_error",
        follow_redirects=True,
    )

    assert 404 == response.status_code
    response_str = response.data
    if isinstance(response.data, bytes):
        response_str = response_str.decode()
    assert json.loads(response_str) == {"url": None, "error": "This is an error"}


def test_extra_links_no_response(dag_run, task_1, viewer_client):
    response = viewer_client.get(
        f"{ENDPOINT}?dag_id={task_1.dag_id}&task_id={task_1.task_id}"
        f"&execution_date={STR_DEFAULT_DATE}&link_name=no_response",
        follow_redirects=True,
    )

    assert response.status_code == 404
    response_str = response.data
    if isinstance(response.data, bytes):
        response_str = response_str.decode()
    assert json.loads(response_str) == {"url": None, "error": "No URL found for no_response"}


def test_operator_extra_link_override_plugin(dag_run, task_2, viewer_client):
    """
    This tests checks if Operator Link (AirflowLink) defined in the Dummy2TestOperator
    is overridden by Airflow Plugin (AirflowLink2).

    AirflowLink returns 'https://airflow.apache.org/' link
    AirflowLink2 returns 'https://airflow.apache.org/1.10.5/' link
    """
    response = viewer_client.get(
        f"{ENDPOINT}?dag_id={task_2.dag_id}&task_id={task_2.task_id}"
        f"&execution_date={STR_DEFAULT_DATE}&link_name=airflow",
        follow_redirects=True,
    )

    assert response.status_code == 200
    response_str = response.data
    if isinstance(response.data, bytes):
        response_str = response_str.decode()
    assert json.loads(response_str) == {"url": "https://airflow.apache.org/1.10.5/", "error": None}


def test_operator_extra_link_multiple_operators(dag_run, task_2, task_3, viewer_client):
    """
    This tests checks if Operator Link (AirflowLink2) defined in
    Airflow Plugin (AirflowLink2) is attached to all the list of
    operators defined in the AirflowLink2().operators property

    AirflowLink2 returns 'https://airflow.apache.org/1.10.5/' link
    GoogleLink returns 'https://www.google.com'
    """
    response = viewer_client.get(
        f"{ENDPOINT}?dag_id={task_2.dag_id}&task_id={task_2.task_id}"
        f"&execution_date={STR_DEFAULT_DATE}&link_name=airflow",
        follow_redirects=True,
    )

    assert response.status_code == 200
    response_str = response.data
    if isinstance(response.data, bytes):
        response_str = response_str.decode()
    assert json.loads(response_str) == {"url": "https://airflow.apache.org/1.10.5/", "error": None}

    response = viewer_client.get(
        f"{ENDPOINT}?dag_id={task_3.dag_id}&task_id={task_3.task_id}"
        f"&execution_date={STR_DEFAULT_DATE}&link_name=airflow",
        follow_redirects=True,
    )

    assert response.status_code == 200
    response_str = response.data
    if isinstance(response.data, bytes):
        response_str = response_str.decode()
    assert json.loads(response_str) == {"url": "https://airflow.apache.org/1.10.5/", "error": None}

    # Also check that the other Operator Link defined for this operator exists
    response = viewer_client.get(
        f"{ENDPOINT}?dag_id={task_3.dag_id}&task_id={task_3.task_id}"
        f"&execution_date={STR_DEFAULT_DATE}&link_name=google",
        follow_redirects=True,
    )

    assert response.status_code == 200
    response_str = response.data
    if isinstance(response.data, bytes):
        response_str = response_str.decode()
    assert json.loads(response_str) == {"url": "https://www.google.com", "error": None}
