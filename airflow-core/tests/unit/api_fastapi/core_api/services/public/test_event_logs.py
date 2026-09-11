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

import pytest
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import set_committed_value

from airflow.api_fastapi.core_api.services.public.event_logs import event_log_to_response
from airflow.models import DagModel, Log
from airflow.utils.session import provide_session

pytestmark = pytest.mark.db_test


def make_event_log(**kwargs) -> Log:
    event_log = Log(event="test", **kwargs)
    event_log.id = 1
    return event_log


def test_event_log_to_response_keeps_stored_owner_display_name():
    event_log = make_event_log(owner="owner", owner_display_name="Stored Owner")

    response = event_log_to_response(event_log=event_log)

    assert response.owner_display_name == "Stored Owner"


def test_event_log_to_response_falls_back_to_owner_when_display_name_is_unset():
    event_log = make_event_log(owner="owner")

    response = event_log_to_response(event_log=event_log)

    assert response.owner_display_name == "owner"


def test_event_log_to_response_keeps_loaded_relationship_display_names():
    event_log = make_event_log(owner="owner", dag_id="my_dag")
    # Mark the relationships as eager-loaded (as the routes do via joinedload) so the unloaded-cleanup
    # leaves them in place instead of nulling them.
    set_committed_value(event_log, "dag_model", DagModel(dag_id="my_dag"))
    set_committed_value(event_log, "task_instance", None)

    response = event_log_to_response(event_log=event_log)

    assert response.dag_display_name == "my_dag"


@provide_session
def test_event_log_to_response_does_not_mark_event_log_dirty(*, session: Session):
    event_log = Log(event="test", owner="owner")
    session.add(event_log)
    session.flush()

    response = event_log_to_response(event_log=event_log)

    assert response.owner_display_name == "owner"
    assert event_log not in session.dirty

    session.delete(event_log)
