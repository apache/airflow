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
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select

from airflow.models.revoked_token import RevokedToken

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_revoked_tokens


class TestRevokedTokenModel:
    def test_jti_column_fits_external_issuer_identifiers(self):
        """The ``jti`` column must hold identifiers longer than Airflow's own 32-char ``uuid4().hex``.

        External identity providers (accepted via ``[api_auth] trusted_jwks_url``) mint ``jti``
        claims such as 36-char RFC 4122 UUIDs; a 32-char column silently drops them on databases
        that enforce length, so revocation never records them.
        """
        assert RevokedToken.__table__.c.jti.type.length == 255
        assert len(str(uuid.uuid4())) <= RevokedToken.__table__.c.jti.type.length

    def test_revoke_inserts_row(self):
        """Test that revoke calls session.merge with a RevokedToken instance."""
        mock_session = MagicMock()
        exp = datetime.now(tz=UTC) + timedelta(hours=1)
        RevokedToken.revoke("test-jti-123", exp, session=mock_session)
        mock_session.merge.assert_called_once()
        arg = mock_session.merge.call_args[0][0]
        assert isinstance(arg, RevokedToken)
        assert arg.jti == "test-jti-123"
        assert arg.exp == exp


FROZEN_NOW = datetime(2026, 11, 1, tzinfo=UTC)
JWT_EXPIRATION_TIME = 3600
CLEANUP_INTERVAL = JWT_EXPIRATION_TIME * 2  # mirrors _maybe_cleanup_expired
LAST_CLEANUP_TIME = 4000.0


@pytest.mark.db_test
@pytest.mark.asyncio
class TestRevokedTokenAsync:
    @pytest.fixture(autouse=True)
    def seed_revoked_tokens(self, session, time_machine):
        time_machine.move_to(FROZEN_NOW, tick=False)
        clear_db_revoked_tokens()
        session.add_all(
            [
                RevokedToken(jti="revoked-jti", exp=FROZEN_NOW + timedelta(days=1)),
                RevokedToken(jti="expired-jti", exp=FROZEN_NOW - timedelta(days=1)),
            ]
        )
        session.commit()
        yield
        clear_db_revoked_tokens()

    @pytest.mark.parametrize(("jti", "expected"), [("revoked-jti", True), ("unknown-jti", False)])
    async def test_is_revoked_async(self, jti, expected):
        with assert_queries_count(0, include_async_engine=False):
            assert await RevokedToken.is_revoked_async(jti) is expected

    # Patch the module's ``time`` name, not ``time.monotonic``.
    # The latter is process-wide and would also freeze the event loop clock.
    @patch("airflow.models.revoked_token.time", autospec=True)
    @patch.object(RevokedToken, "_last_cleanup_time", LAST_CLEANUP_TIME)
    async def test_cleanup_runs_when_interval_passed(self, mock_time, session):
        """Cleanup should run when enough time has passed since last cleanup."""
        mock_time.monotonic.return_value = LAST_CLEANUP_TIME + CLEANUP_INTERVAL

        with conf_vars({("api_auth", "jwt_expiration_time"): str(JWT_EXPIRATION_TIME)}):
            await RevokedToken.is_revoked_async("revoked-jti")

        assert set(session.scalars(select(RevokedToken.jti))) == {"revoked-jti"}

    @patch("airflow.models.revoked_token.time", autospec=True)
    @patch.object(RevokedToken, "_last_cleanup_time", LAST_CLEANUP_TIME)
    async def test_cleanup_skips_when_interval_not_passed(self, mock_time, session):
        """Cleanup should skip when not enough time has passed."""
        mock_time.monotonic.return_value = LAST_CLEANUP_TIME + CLEANUP_INTERVAL - 1

        with conf_vars({("api_auth", "jwt_expiration_time"): str(JWT_EXPIRATION_TIME)}):
            await RevokedToken.is_revoked_async("revoked-jti")

        assert set(session.scalars(select(RevokedToken.jti))) == {"revoked-jti", "expired-jti"}
