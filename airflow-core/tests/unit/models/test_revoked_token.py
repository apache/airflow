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

import math
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import time_machine
from sqlalchemy import select

from airflow.models.revoked_token import RevokedToken

from tests_common.test_utils.asserts import assert_queries_count
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

    def test_is_revoked_returns_true(self):
        """Test that a revoked JTI is detected."""
        mock_session = MagicMock()
        mock_session.scalar.return_value = True
        result = RevokedToken.is_revoked("known-jti", session=mock_session)
        assert result is True

    def test_is_revoked_returns_false(self):
        """Test that an unknown JTI returns False."""
        mock_session = MagicMock()
        mock_session.scalar.return_value = False
        result = RevokedToken.is_revoked("unknown-jti", session=mock_session)
        assert result is False


class TestRevokedTokenCleanup:
    """Tests for automatic periodic cleanup of expired revoked tokens."""

    def test_cleanup_runs_when_interval_passed(self):
        """Cleanup should run when enough time has passed since last cleanup."""
        mock_session = MagicMock()
        mock_session.scalar.return_value = False

        original_last_cleanup = RevokedToken._last_cleanup_time
        try:
            RevokedToken._last_cleanup_time = 0.0
            with (
                patch("airflow.models.revoked_token.time.monotonic", return_value=8000.0),
                patch("airflow.models.revoked_token.conf.getint", return_value=3600),
            ):
                RevokedToken.is_revoked("test-jti", session=mock_session)

            # session.execute should be called for DELETE
            mock_session.execute.assert_called_once()
        finally:
            RevokedToken._last_cleanup_time = original_last_cleanup

    def test_cleanup_skips_when_interval_not_passed(self):
        """Cleanup should skip when not enough time has passed."""
        mock_session = MagicMock()
        mock_session.scalar.return_value = False

        original_last_cleanup = RevokedToken._last_cleanup_time
        try:
            RevokedToken._last_cleanup_time = 4000.0
            # cleanup_interval = 3600 * 2 = 7200, so 4500 - 4000 = 500 < 7200 skips cleanup
            with (
                patch("airflow.models.revoked_token.time.monotonic", return_value=4500.0),
                patch("airflow.models.revoked_token.conf.getint", return_value=3600),
            ):
                RevokedToken.is_revoked("test-jti", session=mock_session)

            # session.execute should NOT be called
            mock_session.execute.assert_not_called()
        finally:
            RevokedToken._last_cleanup_time = original_last_cleanup


FROZEN_NOW = datetime(2026, 11, 1, tzinfo=UTC)


@pytest.mark.db_test
@pytest.mark.asyncio
class TestRevokedTokenAsync:
    @pytest.fixture(autouse=True)
    def cleanup_revoked_tokens(self):
        clear_db_revoked_tokens()
        yield
        clear_db_revoked_tokens()

    @pytest.mark.parametrize(
        ("jti", "cleanup_due", "expected", "expected_remaining"),
        [
            pytest.param("revoked-jti", False, True, {"revoked-jti", "expired-jti"}, id="revoked"),
            pytest.param("unknown-jti", False, False, {"revoked-jti", "expired-jti"}, id="unknown"),
            pytest.param("revoked-jti", True, True, {"revoked-jti"}, id="cleanup-due"),
        ],
    )
    @time_machine.travel(FROZEN_NOW, tick=False)
    async def test_is_revoked_async(self, session, jti, cleanup_due, expected, expected_remaining):
        session.add_all(
            [
                RevokedToken(jti="revoked-jti", exp=FROZEN_NOW + timedelta(days=1)),
                RevokedToken(jti="expired-jti", exp=FROZEN_NOW - timedelta(days=1)),
            ]
        )
        session.commit()

        last_cleanup_time = -math.inf if cleanup_due else math.inf
        with assert_queries_count(0, include_async_engine=False):
            with patch.object(RevokedToken, "_last_cleanup_time", last_cleanup_time):
                assert await RevokedToken.is_revoked_async(jti) is expected

        assert set(session.scalars(select(RevokedToken.jti))) == expected_remaining
