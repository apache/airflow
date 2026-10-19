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
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from airflow.models.revoked_token import RevokedToken
from airflow.utils.session import create_session

from tests_common.test_utils.config import conf_vars


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
        mock_session.scalars.return_value.all.return_value = ["expired-jti"]

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

            mock_session.scalars.assert_not_called()
            mock_session.execute.assert_not_called()
        finally:
            RevokedToken._last_cleanup_time = original_last_cleanup

    def test_cleanup_skipped_while_another_thread_is_cleaning(self):
        """The interval bookkeeping is not thread safe, so only one pass may run at a time."""
        mock_session = MagicMock(spec=Session)
        mock_session.scalar.return_value = False

        original_last_cleanup = RevokedToken._last_cleanup_time
        RevokedToken._cleanup_lock.acquire()
        try:
            RevokedToken._last_cleanup_time = 0.0
            with (
                patch("airflow.models.revoked_token.time.monotonic", return_value=8000.0),
                patch("airflow.models.revoked_token.conf.getint", return_value=3600),
            ):
                assert RevokedToken.is_revoked("test-jti", session=mock_session) is False

            mock_session.scalars.assert_not_called()
            mock_session.execute.assert_not_called()
            # a skipped pass must not claim the interval either
            assert RevokedToken._last_cleanup_time == 0.0
        finally:
            RevokedToken._cleanup_lock.release()
            RevokedToken._last_cleanup_time = original_last_cleanup

    def test_cleanup_skipped_when_another_thread_finished_a_pass_before_the_lock(self):
        mock_session = MagicMock(spec=Session)
        mock_session.scalar.return_value = False

        class LockAcquiredRightAfterAnotherPass:
            def acquire(self, blocking=True):
                RevokedToken._last_cleanup_time = 8000.0
                return True

            def release(self):
                pass

        original_last_cleanup = RevokedToken._last_cleanup_time
        try:
            RevokedToken._last_cleanup_time = 0.0
            with (
                patch.object(RevokedToken, "_cleanup_lock", LockAcquiredRightAfterAnotherPass()),
                patch("airflow.models.revoked_token.time.monotonic", return_value=8000.0),
                patch("airflow.models.revoked_token.conf.getint", return_value=3600),
            ):
                assert RevokedToken.is_revoked("test-jti", session=mock_session) is False

            mock_session.scalars.assert_not_called()
            mock_session.execute.assert_not_called()
        finally:
            RevokedToken._last_cleanup_time = original_last_cleanup

    @pytest.mark.parametrize("failing_statement", ["scalars", "execute"], ids=["select", "delete"])
    def test_failed_cleanup_rolls_back_so_the_revocation_read_still_works(self, failing_statement):
        """A failed statement aborts the transaction on PostgreSQL; the read after it must not inherit that."""
        mock_session = MagicMock(spec=Session)
        mock_session.scalar.return_value = False
        mock_session.scalars.return_value.all.return_value = ["expired-jti"]
        getattr(mock_session, failing_statement).side_effect = RuntimeError("database is on fire")

        original_last_cleanup = RevokedToken._last_cleanup_time
        try:
            RevokedToken._last_cleanup_time = 0.0
            with (
                patch("airflow.models.revoked_token.time.monotonic", return_value=8000.0),
                patch("airflow.models.revoked_token.conf.getint", return_value=3600),
            ):
                assert RevokedToken.is_revoked("test-jti", session=mock_session) is False

            mock_session.rollback.assert_called_once()
            # the lock must not stay held after a failure
            assert RevokedToken._cleanup_lock.acquire(blocking=False)
            RevokedToken._cleanup_lock.release()
        finally:
            RevokedToken._last_cleanup_time = original_last_cleanup


@pytest.mark.db_test
class TestRevokedTokenCleanupIsBounded:
    """Cleanup runs on the request path, so a single pass must not issue an unbounded DELETE."""

    @pytest.fixture(autouse=True)
    def reset_cleanup_state(self):
        original_last_cleanup = RevokedToken._last_cleanup_time
        with create_session() as session:
            session.execute(delete(RevokedToken))
        yield
        RevokedToken._last_cleanup_time = original_last_cleanup
        with create_session() as session:
            session.execute(delete(RevokedToken))

    @staticmethod
    def _add_tokens(expired: int, live: int) -> None:
        now = datetime.now(tz=UTC)
        with create_session() as session:
            for i in range(expired):
                session.add(RevokedToken(jti=f"expired-{i}", exp=now - timedelta(hours=1)))
            for i in range(live):
                session.add(RevokedToken(jti=f"live-{i}", exp=now + timedelta(hours=1)))

    @staticmethod
    def _remaining_jtis() -> set[str]:
        with create_session() as session:
            return set(session.scalars(select(RevokedToken.jti)))

    @conf_vars({("api_auth", "jwt_expiration_time"): "3600"})
    def test_cleanup_deletes_at_most_one_batch(self):
        self._add_tokens(expired=7, live=2)
        RevokedToken._last_cleanup_time = 0.0

        with (
            patch("airflow.models.revoked_token._CLEANUP_BATCH_SIZE", 3),
            patch("airflow.models.revoked_token.time.monotonic", return_value=100_000.0),
        ):
            RevokedToken.is_revoked("live-0")

        remaining = self._remaining_jtis()
        assert len(remaining) == 6
        assert {"live-0", "live-1"} <= remaining

    @conf_vars({("api_auth", "jwt_expiration_time"): "3600"})
    def test_full_batch_lets_the_next_check_resume_draining(self):
        self._add_tokens(expired=7, live=0)
        RevokedToken._last_cleanup_time = 0.0

        with (
            patch("airflow.models.revoked_token._CLEANUP_BATCH_SIZE", 3),
            patch("airflow.models.revoked_token.time.monotonic", return_value=100_000.0),
        ):
            # Each full batch rewinds the interval, so the passes chain on a frozen clock.
            RevokedToken.is_revoked("expired-0")
            assert len(self._remaining_jtis()) == 4
            RevokedToken.is_revoked("expired-0")
            assert len(self._remaining_jtis()) == 1
            RevokedToken.is_revoked("expired-0")
            assert len(self._remaining_jtis()) == 0

            # The last pass did not fill the batch, so the interval applies again
            self._add_tokens(expired=2, live=0)
            RevokedToken.is_revoked("expired-0")
            assert len(self._remaining_jtis()) == 2
