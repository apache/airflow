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

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from airflow.models.revoked_token import RevokedToken


@pytest.fixture(autouse=True)
def _reset_revoked_token_state():
    """Reset class-level cache + cleanup state between tests so they don't leak through.

    Existing tests pre-date the cache and explicitly manage ``_last_cleanup_time``;
    this fixture is additive — it just guarantees a clean slate before each test
    and restores the originals afterward.
    """
    original = (
        RevokedToken._last_cleanup_time,
        RevokedToken._cache,
        RevokedToken._cache_initialized,
    )
    try:
        RevokedToken._last_cleanup_time = 0.0
        RevokedToken._cache = None
        RevokedToken._cache_initialized = False
        yield
    finally:
        (
            RevokedToken._last_cleanup_time,
            RevokedToken._cache,
            RevokedToken._cache_initialized,
        ) = original


def _conf_getint_factory(*, cache_size: int = 10000, ttl: int = 60, jwt_exp: int = 3600):
    """Side-effect for patching ``airflow.models.revoked_token.conf.getint``."""

    def _impl(section: str, key: str, fallback: int = 0) -> int:
        if key == "revoked_token_cache_size":
            return cache_size
        if key == "revoked_token_cache_ttl_seconds":
            return ttl
        if key == "jwt_expiration_time":
            return jwt_exp
        return fallback

    return _impl


class TestRevokedTokenModel:
    def test_revoke_inserts_row(self):
        """Test that revoke calls session.merge with a RevokedToken instance."""
        mock_session = MagicMock()
        exp = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        with patch(
            "airflow.models.revoked_token.conf.getint",
            side_effect=_conf_getint_factory(cache_size=0),
        ):
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
        with patch(
            "airflow.models.revoked_token.conf.getint",
            side_effect=_conf_getint_factory(cache_size=0),
        ):
            result = RevokedToken.is_revoked("known-jti", session=mock_session)
        assert result is True

    def test_is_revoked_returns_false(self):
        """Test that an unknown JTI returns False."""
        mock_session = MagicMock()
        mock_session.scalar.return_value = False
        with patch(
            "airflow.models.revoked_token.conf.getint",
            side_effect=_conf_getint_factory(cache_size=0),
        ):
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
                patch(
                    "airflow.models.revoked_token.conf.getint",
                    side_effect=_conf_getint_factory(cache_size=0),
                ),
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
                patch(
                    "airflow.models.revoked_token.conf.getint",
                    side_effect=_conf_getint_factory(cache_size=0),
                ),
            ):
                RevokedToken.is_revoked("test-jti", session=mock_session)

            # session.execute should NOT be called
            mock_session.execute.assert_not_called()
        finally:
            RevokedToken._last_cleanup_time = original_last_cleanup


class TestRevokedTokenCache:
    """Tests for the in-process ``is_revoked`` cache (3.2 perf-regression fix)."""

    def test_caches_negative_result(self):
        """First miss queries DB; second call within TTL is a cache hit."""
        mock_session = MagicMock()
        mock_session.scalar.return_value = False
        with patch(
            "airflow.models.revoked_token.conf.getint",
            side_effect=_conf_getint_factory(cache_size=10),
        ):
            assert RevokedToken.is_revoked("jti-A", session=mock_session) is False
            assert RevokedToken.is_revoked("jti-A", session=mock_session) is False
        assert mock_session.scalar.call_count == 1

    def test_caches_positive_result(self):
        """Revoked tokens are cached too — short-circuits subsequent rejections."""
        mock_session = MagicMock()
        mock_session.scalar.return_value = True
        with patch(
            "airflow.models.revoked_token.conf.getint",
            side_effect=_conf_getint_factory(cache_size=10),
        ):
            assert RevokedToken.is_revoked("jti-B", session=mock_session) is True
            assert RevokedToken.is_revoked("jti-B", session=mock_session) is True
        assert mock_session.scalar.call_count == 1

    def test_revoke_populates_cache(self):
        """``revoke()`` populates the cache so the same worker is immediately consistent."""
        mock_session = MagicMock()
        # If the cache is bypassed, the DB would say False — proves we hit the cache.
        mock_session.scalar.return_value = False
        exp = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        with patch(
            "airflow.models.revoked_token.conf.getint",
            side_effect=_conf_getint_factory(cache_size=10),
        ):
            RevokedToken.revoke("jti-C", exp, session=mock_session)
            assert RevokedToken.is_revoked("jti-C", session=mock_session) is True
        mock_session.scalar.assert_not_called()

    def test_cache_disabled_when_size_is_zero(self):
        """Setting ``cache_size = 0`` falls through to the DB on every call."""
        mock_session = MagicMock()
        mock_session.scalar.return_value = False
        with patch(
            "airflow.models.revoked_token.conf.getint",
            side_effect=_conf_getint_factory(cache_size=0),
        ):
            RevokedToken.is_revoked("jti-D", session=mock_session)
            RevokedToken.is_revoked("jti-D", session=mock_session)
        assert mock_session.scalar.call_count == 2
        assert RevokedToken._cache is None

    def test_cache_disabled_when_ttl_is_zero(self):
        """Setting ``cache_ttl_seconds = 0`` falls through to the DB on every call."""
        mock_session = MagicMock()
        mock_session.scalar.return_value = False
        with patch(
            "airflow.models.revoked_token.conf.getint",
            side_effect=_conf_getint_factory(ttl=0),
        ):
            RevokedToken.is_revoked("jti-E", session=mock_session)
            RevokedToken.is_revoked("jti-E", session=mock_session)
        assert mock_session.scalar.call_count == 2
        assert RevokedToken._cache is None

    def test_cleanup_runs_even_on_cache_hit(self):
        """``_maybe_cleanup_expired`` must fire on every call, not only on cache misses.

        Locks in the design choice that cleanup runs before the cache lookup so a
        future refactor that short-circuits earlier doesn't silently disable the
        background TTL sweep.
        """
        mock_session = MagicMock()
        mock_session.scalar.return_value = False
        with (
            patch(
                "airflow.models.revoked_token.conf.getint",
                side_effect=_conf_getint_factory(cache_size=10),
            ),
            patch.object(RevokedToken, "_maybe_cleanup_expired") as cleanup,
        ):
            RevokedToken.is_revoked("jti-F", session=mock_session)  # miss
            RevokedToken.is_revoked("jti-F", session=mock_session)  # hit
        assert cleanup.call_count == 2

    def test_clear_cache_returns_count_and_empties(self):
        mock_session = MagicMock()
        mock_session.scalar.return_value = False
        with patch(
            "airflow.models.revoked_token.conf.getint",
            side_effect=_conf_getint_factory(cache_size=10),
        ):
            RevokedToken.is_revoked("jti-G1", session=mock_session)
            RevokedToken.is_revoked("jti-G2", session=mock_session)
            assert RevokedToken.clear_cache() == 2
            assert RevokedToken.clear_cache() == 0  # already empty

    def test_clear_cache_is_noop_when_caching_disabled(self):
        with patch(
            "airflow.models.revoked_token.conf.getint",
            side_effect=_conf_getint_factory(cache_size=0),
        ):
            # Force lazy init so ``_cache_initialized`` flips to True.
            RevokedToken._ensure_cache_initialized()
            assert RevokedToken._cache is None
            assert RevokedToken.clear_cache() == 0

    def test_revoke_does_not_cache_on_db_failure(self):
        """If ``session.merge`` raises, the cache must not be mutated."""
        mock_session = MagicMock()
        mock_session.merge.side_effect = RuntimeError("db down")
        exp = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        with patch(
            "airflow.models.revoked_token.conf.getint",
            side_effect=_conf_getint_factory(cache_size=10),
        ):
            with pytest.raises(RuntimeError):
                RevokedToken.revoke("jti-H", exp, session=mock_session)
        # Cache was either never initialized or initialized but empty.
        assert RevokedToken._cache is None or "jti-H" not in RevokedToken._cache

    def test_cache_lazy_init_is_idempotent(self):
        """Repeat calls to ``_ensure_cache_initialized`` should not rebuild the cache."""
        with patch(
            "airflow.models.revoked_token.conf.getint",
            side_effect=_conf_getint_factory(cache_size=10),
        ):
            RevokedToken._ensure_cache_initialized()
            first_cache = RevokedToken._cache
            RevokedToken._ensure_cache_initialized()
            assert RevokedToken._cache is first_cache
