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

from types import SimpleNamespace
from unittest import mock

import pytest
from sqlalchemy.engine import Result
from sqlalchemy.orm import Session

from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.serialization.definitions.operatorlink import XComOperatorLink


def _keys_queried(session):
    return [call.args[0].compile().params["key_1"] for call in session.execute.call_args_list]


@mock.patch("airflow.serialization.definitions.operatorlink.create_session", autospec=True)
def test_operator_link_lookup_is_bounded(create_session):
    session = mock.create_autospec(Session, instance=True)
    create_session.return_value.__enter__.return_value = session
    result = mock.create_autospec(Result, instance=True)
    result.first.return_value = SimpleNamespace(value='"https://example.com/task"')
    session.execute.return_value = result
    link = XComOperatorLink(name="Example", xcom_key="_link_example")
    ti_key = TaskInstanceKey(dag_id="example", task_id="task", run_id="run", try_number=1, map_index=-1)

    assert link.get_link(None, ti_key=ti_key) == "https://example.com/task"

    session.execute.assert_called_once()
    statement = session.execute.call_args.args[0]
    assert "LIMIT 1" in str(statement.compile(compile_kwargs={"literal_binds": True}))


@mock.patch("airflow.serialization.definitions.operatorlink.create_session", autospec=True)
@pytest.mark.parametrize("try_number", [1, 2])
def test_operator_link_reads_the_requested_try(create_session, try_number):
    """The link shown for a try is the one that try recorded, not the latest one."""
    session = mock.create_autospec(Session, instance=True)
    create_session.return_value.__enter__.return_value = session
    result = mock.create_autospec(Result, instance=True)
    result.first.return_value = SimpleNamespace(value=f'"https://example.com/job?try={try_number}"')
    session.execute.return_value = result
    link = XComOperatorLink(name="Example", xcom_key="_link_example")
    ti_key = TaskInstanceKey(
        dag_id="example", task_id="task", run_id="run", try_number=try_number, map_index=-1
    )

    assert link.get_link(None, ti_key=ti_key) == f"https://example.com/job?try={try_number}"
    assert _keys_queried(session) == [f"_link_example__try_{try_number}"]


@mock.patch("airflow.serialization.definitions.operatorlink.create_session", autospec=True)
def test_operator_link_falls_back_to_unsuffixed_key(create_session):
    """Runs that executed before per try keys existed still resolve their link."""
    session = mock.create_autospec(Session, instance=True)
    create_session.return_value.__enter__.return_value = session
    per_try, legacy = (mock.create_autospec(Result, instance=True) for _ in range(2))
    per_try.first.return_value = None
    legacy.first.return_value = SimpleNamespace(value='"https://example.com/legacy"')
    session.execute.side_effect = [per_try, legacy]
    link = XComOperatorLink(name="Example", xcom_key="_link_example")
    ti_key = TaskInstanceKey(dag_id="example", task_id="task", run_id="run", try_number=2, map_index=-1)

    assert link.get_link(None, ti_key=ti_key) == "https://example.com/legacy"
    assert _keys_queried(session) == ["_link_example__try_2", "_link_example"]
