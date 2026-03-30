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

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, ClassVar

import structlog
from sqlalchemy import String, delete, exists, select
from sqlalchemy.orm import Mapped, mapped_column

from airflow.configuration import conf
from airflow.models.base import Base
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

log = structlog.get_logger(__name__)


class RevokedToken(Base):
    """Stores revoked JWT token JTIs to support token invalidation on logout."""

    __tablename__ = "revoked_token"

    # Track last cleanup time to avoid running cleanup on every request
    _last_cleanup_time: ClassVar[float] = 0.0

    jti: Mapped[str] = mapped_column(String(32), primary_key=True)
    exp: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, index=True)

    @classmethod
    @provide_session
    def revoke(cls, jti: str, exp: datetime, session: Session = NEW_SESSION) -> None:
        """Add a token JTI to the revoked tokens."""
        session.merge(cls(jti=jti, exp=exp))

    @classmethod
    @provide_session
    def is_revoked(cls, jti: str, session: Session = NEW_SESSION) -> bool:
        """Check if a token JTI has been revoked."""
        cls._maybe_cleanup_expired(session)
        return bool(session.scalar(select(exists().where(cls.jti == jti))))

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
