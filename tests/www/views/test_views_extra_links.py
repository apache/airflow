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
import datetime
import json
import re
from unittest import mock

import pytest

from airflow.models import DAG, TaskInstance
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.utils import dates, timezone
from airflow.utils.session import create_session
from tests.test_utils.mock_operators import Dummy2TestOperator, Dummy3TestOperator
from tests.test_utils.www import check_content_in_response

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

ENDPOINT = "extra_links"


class RaiseErrorLink(BaseOperatorLink):
    name = 'raise_error'

    def get_link(self, operator, dttm):
        raise ValueError('This is an error')


class NoResponseLink(BaseOperatorLink):
    name = 'no_response'

    def get_link(self, operator, dttm):
        return None


class FooBarLink(BaseOperatorLink):
    name = 'foo-bar'

    def get_link(self, operator, dttm):
        return f"http://www.example.com/{operator.task_id}/foo-bar/{dttm}"


class AirflowLink(BaseOperatorLink):
    name = 'airflow'

    def get_link(self, operator, dttm):
        return 'https://airflow.apache.org'


class DummyTestOperator(BaseOperator):
    operator_extra_links = (
        RaiseErrorLink(),
        NoResponseLink(),
        FooBarLink(),
        AirflowLink(),
    )


@pytest.fixture(scope="module")
def dag():
    return DAG("dag", start_date=DEFAULT_DATE)


@pytest.fixture(scope="module", autouse=True)
def patched_app(app, dag):
    with mock.patch.object(app, "dag_bag") as mock_dag_bag:
        mock_dag_bag.get_dag.return_value = dag
        yield


@pytest.fixture(scope="module", autouse=True)
def task_1(dag):
    return DummyTestOperator(task_id="some_dummy_task", dag=dag)


@pytest.fixture(scope="module", autouse=True)
def task_2(dag):
    return Dummy2TestOperator(task_id="some_dummy_task_2", dag=dag)


@pytest.fixture(scope="module", autouse=True)
def task_3(dag):
    return Dummy3TestOperator(task_id="some_dummy_task_3", dag=dag)


@pytest.fixture(scope="module", autouse=True)
def init_blank_task_instances():
    """Make sure there are no runs before we test anything.

    This really shouldn't be needed, but tests elsewhere leave the db dirty.
    """
    with create_session() as session:
        session.query(TaskInstance).delete()


@pytest.fixture(autouse=True)
def reset_task_instances():
    yield
    with create_session() as session:
        session.query(TaskInstance).delete()


def test_extra_links_works(dag, task_1, viewer_client):
    response = viewer_client.get(
        "{}?dag_id={}&task_id={}&execution_date={}&link_name=foo-bar".format(
            ENDPOINT,
            dag.dag_id,
            task_1.task_id,
            DEFAULT_DATE,
        ),
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert json.loads(response.data.decode()) == {
        'url': 'http://www.example.com/some_dummy_task/foo-bar/2017-01-01T00:00:00+00:00',
        'error': None,
    }


def test_global_extra_links_works(dag, task_1, viewer_client):
    response = viewer_client.get(
        "{}?dag_id={}&task_id={}&execution_date={}&link_name=github".format(
            ENDPOINT,
            dag.dag_id,
            task_1.task_id,
            DEFAULT_DATE,
        ),
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert json.loads(response.data.decode()) == {
        'url': 'https://github.com/apache/airflow',
        'error': None,
    }


def test_extra_link_in_gantt_view(dag, viewer_client):
    exec_date = dates.days_ago(2)
    start_date = timezone.datetime(2020, 4, 10, 2, 0, 0)
    end_date = exec_date + datetime.timedelta(seconds=30)

    with create_session() as session:
        for task in dag.tasks:
            ti = TaskInstance(task=task, execution_date=exec_date, state="success")
            ti.start_date = start_date
            ti.end_date = end_date
            session.add(ti)

    url = f'gantt?dag_id={dag.dag_id}&execution_date={exec_date}'
    resp = viewer_client.get(url, follow_redirects=True)

    check_content_in_response('"extraLinks":', resp)

    extra_links_grps = re.search(r'extraLinks\": \[(\".*?\")\]', resp.get_data(as_text=True))
    extra_links = extra_links_grps.group(0)
    assert 'airflow' in extra_links
    assert 'github' in extra_links


def test_operator_extra_link_override_global_extra_link(dag, task_1, viewer_client):
    response = viewer_client.get(
        "{}?dag_id={}&task_id={}&execution_date={}&link_name=airflow".format(
            ENDPOINT,
            dag.dag_id,
            task_1.task_id,
            DEFAULT_DATE,
        ),
        follow_redirects=True,
    )

    assert response.status_code == 200
    response_str = response.data
    if isinstance(response.data, bytes):
        response_str = response_str.decode()
    assert json.loads(response_str) == {'url': 'https://airflow.apache.org', 'error': None}


def test_extra_links_error_raised(dag, task_1, viewer_client):
    response = viewer_client.get(
        "{}?dag_id={}&task_id={}&execution_date={}&link_name=raise_error".format(
            ENDPOINT,
            dag.dag_id,
            task_1.task_id,
            DEFAULT_DATE,
        ),
        follow_redirects=True,
    )

    assert 404 == response.status_code
    response_str = response.data
    if isinstance(response.data, bytes):
        response_str = response_str.decode()
    assert json.loads(response_str) == {'url': None, 'error': 'This is an error'}


def test_extra_links_no_response(dag, task_1, viewer_client):
    response = viewer_client.get(
        "{}?dag_id={}&task_id={}&execution_date={}&link_name=no_response".format(
            ENDPOINT,
            dag.dag_id,
            task_1.task_id,
            DEFAULT_DATE,
        ),
        follow_redirects=True,
    )

    assert response.status_code == 404
    response_str = response.data
    if isinstance(response.data, bytes):
        response_str = response_str.decode()
    assert json.loads(response_str) == {'url': None, 'error': 'No URL found for no_response'}


def test_operator_extra_link_override_plugin(dag, task_2, viewer_client):
    """
    This tests checks if Operator Link (AirflowLink) defined in the Dummy2TestOperator
    is overridden by Airflow Plugin (AirflowLink2).

    AirflowLink returns 'https://airflow.apache.org/' link
    AirflowLink2 returns 'https://airflow.apache.org/1.10.5/' link
    """
    response = viewer_client.get(
        "{}?dag_id={}&task_id={}&execution_date={}&link_name=airflow".format(
            ENDPOINT,
            dag.dag_id,
            task_2.task_id,
            DEFAULT_DATE,
        ),
        follow_redirects=True,
    )

    assert response.status_code == 200
    response_str = response.data
    if isinstance(response.data, bytes):
        response_str = response_str.decode()
    assert json.loads(response_str) == {'url': 'https://airflow.apache.org/1.10.5/', 'error': None}


def test_operator_extra_link_multiple_operators(dag, task_2, task_3, viewer_client):
    """
    This tests checks if Operator Link (AirflowLink2) defined in
    Airflow Plugin (AirflowLink2) is attached to all the list of
    operators defined in the AirflowLink2().operators property

    AirflowLink2 returns 'https://airflow.apache.org/1.10.5/' link
    GoogleLink returns 'https://www.google.com'
    """
    response = viewer_client.get(
        "{}?dag_id={}&task_id={}&execution_date={}&link_name=airflow".format(
            ENDPOINT,
            dag.dag_id,
            task_2.task_id,
            DEFAULT_DATE,
        ),
        follow_redirects=True,
    )

    assert response.status_code == 200
    response_str = response.data
    if isinstance(response.data, bytes):
        response_str = response_str.decode()
    assert json.loads(response_str) == {'url': 'https://airflow.apache.org/1.10.5/', 'error': None}

    response = viewer_client.get(
        "{}?dag_id={}&task_id={}&execution_date={}&link_name=airflow".format(
            ENDPOINT,
            dag.dag_id,
            task_3.task_id,
            DEFAULT_DATE,
        ),
        follow_redirects=True,
    )

    assert response.status_code == 200
    response_str = response.data
    if isinstance(response.data, bytes):
        response_str = response_str.decode()
    assert json.loads(response_str) == {'url': 'https://airflow.apache.org/1.10.5/', 'error': None}

    # Also check that the other Operator Link defined for this operator exists
    response = viewer_client.get(
        "{}?dag_id={}&task_id={}&execution_date={}&link_name=google".format(
            ENDPOINT,
            dag.dag_id,
            task_3.task_id,
            DEFAULT_DATE,
        ),
        follow_redirects=True,
    )

    assert response.status_code == 200
    response_str = response.data
    if isinstance(response.data, bytes):
        response_str = response_str.decode()
    assert json.loads(response_str) == {'url': 'https://www.google.com', 'error': None}
