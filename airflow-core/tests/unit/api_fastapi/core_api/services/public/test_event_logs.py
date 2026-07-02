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

from airflow.api_fastapi.core_api.services.public.event_logs import event_log_to_response
from airflow.models import Log
from airflow.utils.session import provide_session

pytestmark = pytest.mark.db_test


class User:
    def __init__(self, username: str, first_name: str | None = None, last_name: str | None = None):
        self.username = username
        self.first_name = first_name
        self.last_name = last_name

    def get_id(self) -> str:
        return self.username

    def get_name(self) -> str:
        return self.username


def test_event_log_to_response_keeps_stored_owner_display_name():
    event_log = Log(event="test", owner="owner", owner_display_name="Stored Owner")

    response = event_log_to_response(event_log=event_log, user=User("owner", "Current", "Owner"))

    assert response.owner_display_name == "Stored Owner"


def test_event_log_to_response_resolves_current_user_display_name():
    event_log = Log(event="test", owner="owner")

    response = event_log_to_response(event_log=event_log, user=User("owner", "Current", "Owner"))

    assert response.owner_display_name == "Current Owner"


def test_event_log_to_response_falls_back_to_owner_when_user_does_not_match():
    event_log = Log(event="test", owner="owner")

    response = event_log_to_response(event_log=event_log, user=User("other-owner", "Current", "Owner"))

    assert response.owner_display_name == "owner"


@provide_session
def test_event_log_to_response_does_not_mark_event_log_dirty(*, session: Session):
    event_log = Log(event="test", owner="owner")
    session.add(event_log)
    session.flush()

    try:
        response = event_log_to_response(event_log=event_log, user=User("owner", "Current", "Owner"))

        assert response.owner_display_name == "Current Owner"
        assert event_log not in session.dirty
    finally:
        session.rollback()
