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

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from airflow.models.inbound_delivery import InboundDelivery

from tests_common.test_utils.db import clear_db_inbound_deliveries

pytestmark = pytest.mark.db_test

RECEIVED_AT = datetime(2026, 10, 18, tzinfo=UTC)


def build_delivery(**overrides: Any) -> InboundDelivery:
    fields: dict[str, Any] = {
        "webhook": "github",
        "delivery_id": "d-1",
        "body_digest": "0" * 64,
        "received_at": RECEIVED_AT,
        "retain_until": RECEIVED_AT + timedelta(days=7),
        "source_type": "github",
        "outcome": "no_subscribers",
    }
    return InboundDelivery(**(fields | overrides))


class TestInboundDelivery:
    def setup_method(self):
        clear_db_inbound_deliveries()

    def test_retry_of_a_delivery_is_rejected(self, session):
        session.add(build_delivery())
        session.flush()

        session.add(build_delivery())
        with pytest.raises(IntegrityError):
            session.flush()

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            pytest.param({"delivery_id": None}, {"delivery_id": None}, id="no-delivery-id"),
            pytest.param({"webhook": "github"}, {"webhook": "gitlab"}, id="same-id-on-two-webhooks"),
            pytest.param(
                {"body_digest": "0" * 64}, {"body_digest": "1" * 64}, id="same-id-with-different-bodies"
            ),
        ],
    )
    def test_deliveries_with_distinct_keys_are_both_recorded(self, session, first, second):
        session.add_all([build_delivery(**first), build_delivery(**second)])
        session.flush()

        assert session.scalar(select(func.count()).select_from(InboundDelivery)) == 2

    @pytest.mark.backend("mysql")
    def test_delivery_ids_differing_only_in_case_are_both_recorded(self, session):
        # MySQL compares ids case-insensitively unless the column has a binary collation.
        session.add_all([build_delivery(delivery_id="ABC"), build_delivery(delivery_id="abc")])
        session.flush()

        assert session.scalar(select(func.count()).select_from(InboundDelivery)) == 2

    @pytest.mark.parametrize("column", ["attributes", "candidates", "payload", "actions"])
    def test_explicit_none_in_a_json_column_is_stored_as_sql_null(self, session, column):
        session.add(build_delivery(**{column: None}))
        session.flush()

        query = (
            select(func.count())
            .select_from(InboundDelivery)
            .where(getattr(InboundDelivery, column).is_(None))
        )
        assert session.scalar(query) == 1
