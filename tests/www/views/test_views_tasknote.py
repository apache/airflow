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

import pytest

from airflow.models import DagBag, DagRun, TaskInstance, TaskNote
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from tests.test_utils.www import check_content_in_response


@pytest.fixture(scope="module", autouse=True)
def asdf():
    with create_session() as session:
        session.query(TaskNote).delete()
        session.query(TaskInstance).delete()
        session.query(DagRun).delete()

    yield
    with create_session() as session:
        session.query(TaskInstance).delete()
        session.query(DagRun).delete()
        session.query(TaskNote).delete()


def create_dag_and_task_instance(session, dag_id, task_id, execution_date):
    dag = DagBag().get_dag(dag_id)
    dag.sync_to_db(session=session)
    task = dag.get_task(task_id=task_id)

    task_instance = TaskInstance(task, execution_date, State.SUCCESS)
    session.add(task_instance)
    session.commit()


def test_submit_task_note(session, admin_client):
    test_dag_id = "example_bash_operator"
    test_task_id = "runme_0"
    str_execution_date = "2020-06-10T20:00:00"
    execution_date = timezone.parse(str_execution_date)
    task_note = "this very important note"

    create_dag_and_task_instance(
        session=session, dag_id=test_dag_id, task_id=test_task_id, execution_date=execution_date
    )

    admin_client.post(
        f'task_note?dag_id={test_dag_id}&task_id={test_task_id}&execution_date={str_execution_date}',
        data={"note": task_note},
    )

    note = TaskNote.get_many(
        dag_ids=test_dag_id, task_ids=test_task_id, execution_date=execution_date
    ).first()

    assert note is not None and note.task_note == task_note


def test_submit_task_note_not_ascii(session, admin_client):
    test_dag_id = "example_bash_operator"
    test_task_id = "runme_0"
    str_execution_date = "2020-06-11T20:00:00"
    execution_date = timezone.parse(str_execution_date)
    task_note = "你好"

    create_dag_and_task_instance(
        session=session, dag_id=test_dag_id, task_id=test_task_id, execution_date=execution_date
    )

    admin_client.post(
        f'task_note?dag_id={test_dag_id}&task_id={test_task_id}&execution_date={str_execution_date}',
        data={"note": task_note},
    )

    note = TaskNote.get_many(
        dag_ids=test_dag_id, task_ids=test_task_id, execution_date=execution_date
    ).first()

    assert note is not None and note.task_note == task_note


def test_submit_task_note_not_existing_task(session, admin_client):
    test_dag_id = "example_bash_operator"
    test_task_id = "runme_0"
    str_execution_date = "2020-06-12T20:00:00"
    str_wrong_execution_date = "2020-05-13T20:00:00"
    execution_date = timezone.parse(str_execution_date)

    task_note = "this very important note"

    create_dag_and_task_instance(
        session=session, dag_id=test_dag_id, task_id=test_task_id, execution_date=execution_date
    )

    resp = admin_client.post(
        f'task_note?dag_id={test_dag_id}&task_id={test_task_id}&execution_date={str_wrong_execution_date}',
        data={"note": task_note},
    )

    assert 302 == resp.status_code


def test_get_task_note(admin_client, session):
    test_dag_id = "example_bash_operator"
    test_task_id = "runme_0"
    str_execution_date = "2020-06-15T20:00:00"
    execution_date = timezone.parse(str_execution_date)
    task_note = "this very important note"

    create_dag_and_task_instance(
        session=session, dag_id=test_dag_id, task_id=test_task_id, execution_date=execution_date
    )

    assert session.query(TaskInstance).first() is not None

    TaskNote.set(
        dag_id=test_dag_id,
        task_id=test_task_id,
        execution_date=execution_date,
        timestamp=timezone.utcnow(),
        user_name="User",
        task_note=task_note,
    )

    resp = admin_client.get(
        f'task_note?dag_id={test_dag_id}&task_id={test_task_id}&execution_date={str_execution_date}',
        follow_redirects=True,
    )

    check_content_in_response(task_note, resp, 200)


def test_list_notes(admin_client, session):
    test_dag_id = "example_bash_operator"
    test_task_id = "runme_0"
    str_execution_date = "2020-06-17T20:00:00"
    execution_date = timezone.parse(str_execution_date)
    task_note = "this very important note"

    create_dag_and_task_instance(
        session=session, dag_id=test_dag_id, task_id=test_task_id, execution_date=execution_date
    )

    assert session.query(TaskInstance).first() is not None

    TaskNote.set(
        dag_id=test_dag_id,
        task_id=test_task_id,
        execution_date=execution_date,
        timestamp=timezone.utcnow(),
        user_name="User",
        task_note=task_note,
    )

    resp = admin_client.get('/notes/list', follow_redirects=True)
    check_content_in_response(task_note, resp, 200)
