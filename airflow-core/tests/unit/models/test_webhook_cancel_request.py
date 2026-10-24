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

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError

from airflow.models.dag import DagModel
from airflow.models.webhook_cancel_request import CancelRequestStatus, WebhookCancelRequest

from tests_common.test_utils.db import clear_db_dags, clear_db_webhook_cancel_requests

DAG_ID = "cancellable"
RECEIVED_AT = datetime(2026, 10, 23, tzinfo=UTC)
INBOUND_DELIVERY_ID = uuid.UUID("01a14ee2-0e00-7366-b2e2-38ec29a5a228")


def build_request(**overrides: Any) -> WebhookCancelRequest:
    fields: dict[str, Any] = {
        "inbound_delivery_id": INBOUND_DELIVERY_ID,
        "delivery_id": "d-1",
        "webhook": "github",
        "correlation_key": "my-org/web#42",
        "received_at": RECEIVED_AT,
        "dag_id": DAG_ID,
        "targets": {"run": {"correlation_key_confs": []}, "tasks": []},
        "retain_until": RECEIVED_AT + timedelta(days=7),
    }
    return WebhookCancelRequest(**(fields | overrides))


def count_requests(session) -> int:
    return session.scalar(select(func.count()).select_from(WebhookCancelRequest))


class TestCancelRequestStatus:
    @pytest.mark.parametrize("status", list(CancelRequestStatus))
    def test_formats_as_its_value(self, status):
        assert str(status) == f"{status}" == status.value


@pytest.mark.db_test
class TestWebhookCancelRequest:
    @pytest.fixture(autouse=True)
    def dag_model(self, testing_dag_bundle, session):
        clear_db_dags()
        session.add(DagModel(dag_id=DAG_ID, bundle_name="testing"))
        session.flush()
        yield
        clear_db_webhook_cancel_requests()
        clear_db_dags()

    def test_a_new_request_is_pending(self, session):
        request = build_request()
        session.add(request)
        session.flush()

        assert (request.status, request.applied_at, request.result) == (
            CancelRequestStatus.PENDING,
            None,
            None,
        )

    def test_a_second_request_for_a_dag_from_one_delivery_is_rejected(self, session):
        session.add(build_request())
        session.flush()

        session.add(build_request(correlation_key="my-org/web#43"))
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()

    def test_requests_are_deleted_with_their_dag(self, session):
        session.add(build_request())
        session.flush()

        session.execute(delete(DagModel).where(DagModel.dag_id == DAG_ID))

        assert count_requests(session) == 0

    def test_explicit_none_result_is_stored_as_sql_null(self, session):
        session.add(build_request(result=None))
        session.flush()

        query = (
            select(func.count())
            .select_from(WebhookCancelRequest)
            .where(WebhookCancelRequest.result.is_(None))
        )
        assert session.scalar(query) == 1
