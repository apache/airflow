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

import pytest

from airflow.listeners.listener import get_listener_manager
from airflow.models.log import Log

from unit.listeners import audit_log_listener

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def clean_listener_state():
    yield
    audit_log_listener.clear()


def test_audit_log_hookspec_registered():
    """Verify the on_audit_log_created hookspec is registered in the listener manager."""
    lm = get_listener_manager()
    assert hasattr(lm.hook, "on_audit_log_created")


def test_audit_log_listener_receives_event(listener_manager):
    """Verify a registered listener receives audit log events."""
    listener_manager(audit_log_listener)

    log = Log(event="test_event", owner="test_user")
    get_listener_manager().hook.on_audit_log_created(log=log)

    assert len(audit_log_listener.audit_logs) == 1
    assert audit_log_listener.audit_logs[0].event == "test_event"
    assert audit_log_listener.audit_logs[0].owner == "test_user"


def test_audit_log_listener_no_event_without_registration():
    """Verify no error when hook fires with no listeners registered."""
    log = Log(event="test_event", owner="test_user")
    # Should not raise
    get_listener_manager().hook.on_audit_log_created(log=log)
    assert len(audit_log_listener.audit_logs) == 0
