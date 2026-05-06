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

import threading
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, ClassVar

import structlog
from cachetools import TTLCache
from sqlalchemy import String, delete, exists, select
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.observability.metrics import stats
from airflow.configuration import conf
from airflow.models.base import Base
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

log = structlog.get_logger(__name__)

# Sentinel for cache-miss detection — a `None` value would clash with a valid bool.
_CACHE_MISS = object()


class RevokedToken(Base):
    """Stores revoked JWT token JTIs to support token invalidation on logout.

    The class also exposes a process-local TTL cache for ``is_revoked`` lookups.
    Every authenticated API request goes through ``BaseAuthManager.get_user_from_token``,
    which calls ``RevokedToken.is_revoked(jti)``. Without caching, that means a
    SQLAlchemy roundtrip per request — at modest concurrency (UI polling, fan-out
    DAGs) the connection pool exhausts and request handlers time out in
    ``QueuePool._do_get``. Cache hit rate is ≈100% in practice (revocation only
    happens on explicit logout), so an in-memory lookup keyed by ``jti`` collapses
    the per-request DB roundtrip into a near-free check.

    The cache is bounded by ``[api_auth] revoked_token_cache_size`` (default 10000)
    and entries expire after ``[api_auth] revoked_token_cache_ttl_seconds``
    (default 60). Setting either to 0 disables caching and reverts to the
    per-request DB query behavior.

    The cache is per-process. uvicorn API server workers do not share memory, so
    ``revoke()`` populates only the local worker's cache. Other workers learn of
    the revocation when their cached entry expires (worst case: the configured
    TTL). Operators needing strict cross-worker logout consistency can set
    ``revoked_token_cache_ttl_seconds = 0`` at the cost of restoring the
    per-request DB roundtrip.

    Implementation note: the cache assumes JWT ``exp`` validation rejects expired
    tokens BEFORE this method runs (see ``BaseAuthManager.get_user_from_token``
    -> ``JWTValidator.avalidated_claims``). A custom auth-manager subclass that
    bypasses that validation must do its own ``exp`` check before relying on
    the result here.
    """

    __tablename__ = "revoked_token"

    # Track last cleanup time to avoid running cleanup on every request
    _last_cleanup_time: ClassVar[float] = 0.0

    # In-process cache for ``is_revoked`` lookups, populated lazily on first use.
    # ``None`` when caching is disabled (size or ttl is 0). Mutations are guarded
    # by ``_cache_lock`` because cachetools is not internally thread-safe — its
    # LRU/TTL bookkeeping mutates linked-list state on every access.
    _cache: ClassVar[TTLCache | None] = None
    _cache_lock: ClassVar[threading.RLock] = threading.RLock()
    _cache_initialized: ClassVar[bool] = False

    jti: Mapped[str] = mapped_column(String(32), primary_key=True)
    exp: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, index=True)

    @classmethod
    @provide_session
    def revoke(cls, jti: str, exp: datetime, session: Session = NEW_SESSION) -> None:
        """Add a token JTI to the revoked tokens.

        Also populates the local in-process cache so the same worker
        immediately sees the revocation without a follow-up DB query. Other
        workers' caches learn of the revocation when their cached entry
        expires (worst case: ``revoked_token_cache_ttl_seconds``).

        If ``session.merge`` raises, the cache is left untouched.
        """
        session.merge(cls(jti=jti, exp=exp))
        cls._ensure_cache_initialized()
        if cls._cache is not None:
            with cls._cache_lock:
                cls._cache[jti] = True

    @classmethod
    @provide_session
    def is_revoked(cls, jti: str, session: Session = NEW_SESSION) -> bool:
        """Check if a token JTI has been revoked, using a process-local TTL cache."""
        # Run cleanup BEFORE the cache lookup so periodic deletion of expired
        # rows still fires when most calls are cache hits. ``_maybe_cleanup_expired``
        # is self-throttled by ``_last_cleanup_time``, so the cost on hot paths
        # is one ``time.monotonic()`` comparison.
        cls._maybe_cleanup_expired(session)
        cls._ensure_cache_initialized()
        cache = cls._cache
        if cache is None:
            # Caching disabled — original 3.2 behavior.
            return bool(session.scalar(select(exists().where(cls.jti == jti))))

        with cls._cache_lock:
            cached = cache.get(jti, _CACHE_MISS)
        if cached is not _CACHE_MISS:
            stats.incr("api_auth.revoked_token.cache_hit")
            return cached  # type: ignore[return-value]

        # Cache miss — query DB.
        db_result = bool(session.scalar(select(exists().where(cls.jti == jti))))

        # Double-checked locking: another thread may have populated the entry
        # while we were querying. Prefer their value and avoid double-counting
        # the miss metric. (Cold-cache thundering herd is bounded by worker
        # concurrency and self-corrects after the first populate.)
        with cls._cache_lock:
            re_cached = cache.get(jti, _CACHE_MISS)
            if re_cached is not _CACHE_MISS:
                stats.incr("api_auth.revoked_token.cache_hit")
                return re_cached  # type: ignore[return-value]
            cache[jti] = db_result
            cache_size = len(cache)
        stats.incr("api_auth.revoked_token.cache_miss")
        stats.gauge("api_auth.revoked_token.cache_size", cache_size, rate=0.1)
        return db_result

    @classmethod
    def clear_cache(cls) -> int:
        """Clear the in-process cache and return the number of evicted entries.

        No-op (returns 0) when caching is disabled. Useful for tests and for
        operators wanting to force a refresh from the database after an
        out-of-band revocation.
        """
        if cls._cache is None:
            return 0
        with cls._cache_lock:
            count = len(cls._cache)
            cls._cache.clear()
        stats.incr("api_auth.revoked_token.cache_clear")
        stats.gauge("api_auth.revoked_token.cache_size", 0)
        return count

    @classmethod
    def _maybe_cleanup_expired(cls, session: Session) -> None:
        """
        Periodically clean up expired revoked tokens.

        Cleanup interval is based on jwt_expiration_time config to ensure expired
        tokens are cleaned up after they're no longer useful. Uses monotonic time
        to track intervals.
        """
        now = time.monotonic()
        cleanup_interval = conf.getint("api_auth", "jwt_expiration_time", fallback=3600) * 2
        if now - cls._last_cleanup_time >= cleanup_interval:
            cls._last_cleanup_time = now
            try:
                session.execute(delete(cls).where(cls.exp < datetime.now(tz=timezone.utc)))
            except Exception:
                log.exception("Failed to clean up expired revoked tokens")

    @classmethod
    def _ensure_cache_initialized(cls) -> None:
        """Lazily build the in-process cache from config the first time it's needed.

        Idempotent and thread-safe. If ``[api_auth] revoked_token_cache_size`` or
        ``revoked_token_cache_ttl_seconds`` is 0, leaves ``_cache`` as ``None``
        and every call falls through to the DB.
        """
        if cls._cache_initialized:
            return
        with cls._cache_lock:
            if cls._cache_initialized:
                return
            cache_size = conf.getint("api_auth", "revoked_token_cache_size", fallback=10000)
            cache_ttl = conf.getint("api_auth", "revoked_token_cache_ttl_seconds", fallback=60)
            if cache_size > 0 and cache_ttl > 0:
                cls._cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)
            cls._cache_initialized = True
