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

import functools
import logging
import time
from collections.abc import Callable, Iterator
from inspect import signature
from typing import TYPE_CHECKING, TypeVar, overload

from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm.exc import StaleDataError

from airflow.configuration import conf

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow._shared.logging.types import Logger

log = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable)

MAX_DB_RETRIES = conf.getint("database", "max_db_retries", fallback=3)


def run_with_db_retries(max_retries: int = MAX_DB_RETRIES, logger: Logger | None = None, **kwargs):
    """Return Tenacity Retrying object with project specific default."""
    import tenacity

    # Default kwargs
    retry_kwargs = dict(
        retry=tenacity.retry_if_exception_type(exception_types=(DBAPIError, StaleDataError)),
        wait=tenacity.wait_random_exponential(multiplier=0.5, max=5),
        stop=tenacity.stop_after_attempt(max_retries),
        reraise=True,
        **kwargs,
    )
    if logger is not None:
        retry_kwargs["before_sleep"] = tenacity.before_sleep_log(logger, logging.DEBUG, True)

    return tenacity.Retrying(**retry_kwargs)


@overload
def retry_db_transaction(*, retries: int = MAX_DB_RETRIES) -> Callable[[F], F]: ...


@overload
def retry_db_transaction(_func: F) -> F: ...


def retry_db_transaction(_func: Callable | None = None, *, retries: int = MAX_DB_RETRIES, **retry_kwargs):
    """
    Retry functions in case of ``DBAPIError`` from DB.

    It should not be used with ``@provide_session``.
    """

    def retry_decorator(func: Callable) -> Callable:
        # Get Positional argument for 'session'
        func_params = signature(func).parameters
        try:
            # func_params is an ordered dict -- this is the "recommended" way of getting the position
            session_args_idx = tuple(func_params).index("session")
        except ValueError:
            raise ValueError(f"Function {func.__qualname__} has no `session` argument")
        # We don't need this anymore -- ensure we don't keep a reference to it by mistake
        del func_params

        @functools.wraps(func)
        def wrapped_function(*args, **kwargs):
            if args and hasattr(args[0], "logger"):
                logger = args[0].logger()
            elif args and hasattr(args[0], "log"):
                logger = args[0].log
            else:
                logger = logging.getLogger(func.__module__)
            # Get session from args or kwargs
            if "session" in kwargs:
                session = kwargs["session"]
            elif len(args) > session_args_idx:
                session = args[session_args_idx]
            else:
                raise TypeError(f"session is a required argument for {func.__qualname__}")

            for attempt in run_with_db_retries(max_retries=retries, logger=logger, **retry_kwargs):
                with attempt:
                    logger.debug(
                        "Running %s with retries. Try %d of %d",
                        func.__qualname__,
                        attempt.retry_state.attempt_number,
                        retries,
                    )
                    try:
                        return func(*args, **kwargs)
                    except (DBAPIError, StaleDataError):
                        session.rollback()
                        raise

        return wrapped_function

    # Allow using decorator with and without arguments
    if _func is None:
        return retry_decorator
    return retry_decorator(_func)


#: PostgreSQL ``deadlock_detected``.
PG_DEADLOCK_SQLSTATE = "40P01"
#: MySQL ``ER_LOCK_DEADLOCK``.
MYSQL_DEADLOCK_ERRNO = 1213
#: MySQL ``ER_SP_DOES_NOT_EXIST`` for SAVEPOINT. Never retryable on its own -- see below.
MYSQL_MISSING_SAVEPOINT_ERRNO = 1305

MAX_ATTEMPTS = 3
#: Short, bounded, and jitter-free: these transactions are already holding locks, so the point
#: is to let the winner finish, not to wait out a busy system.
BACKOFF_SECONDS = (0.05, 0.2)


def _driver_codes(exc: BaseException) -> tuple[str | None, int | None]:
    """Return ``(sqlstate, errno)`` from a DBAPI exception, by diagnostics only."""
    orig = getattr(exc, "orig", None)
    if orig is None:
        return None, None
    # psycopg3 exposes ``sqlstate``; psycopg2 exposes ``pgcode``.
    sqlstate = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
    errno = None
    args = getattr(orig, "args", ())
    if args and isinstance(args[0], int):
        errno = args[0]
    return sqlstate, errno


def is_deadlock(exc: BaseException) -> bool:
    """
    Whether ``exc`` -- or anything in its cause chain -- is a database deadlock.

    The chain walk is load-bearing on MySQL. When InnoDB picks a transaction as the deadlock
    victim it rolls the whole transaction back, so the ``RELEASE SAVEPOINT`` SQLAlchemy issues
    while unwinding fails with ``1305`` and *that* becomes the exception the caller sees. The
    original ``1213`` survives only as ``__cause__``/``__context__``.

    A bare ``1305`` is **not** retryable. It means a SAVEPOINT was missing, which happens for
    reasons other than deadlock and which replay would not fix.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        sqlstate, errno = _driver_codes(current)
        if sqlstate == PG_DEADLOCK_SQLSTATE or errno == MYSQL_DEADLOCK_ERRNO:
            return True
        current = current.__cause__ or current.__context__
    return False


def original_deadlock(exc: BaseException) -> BaseException:
    """Return the deadlock in ``exc``'s chain, so diagnostics are not reported as ``1305``."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        sqlstate, errno = _driver_codes(current)
        if sqlstate == PG_DEADLOCK_SQLSTATE or errno == MYSQL_DEADLOCK_ERRNO:
            return current
        current = current.__cause__ or current.__context__
    return exc


class _ReplayState:
    """Shared between the loop and the per-attempt context managers."""

    def __init__(self, max_attempts: int) -> None:
        self.max_attempts = max_attempts
        self.succeeded = False
        self.exhausted = False
        self.last: BaseException | None = None


class _Attempt:
    """
    One attempt. Suppresses a deadlock so the loop can run again; propagates anything else.

    This has to be a context manager rather than a bare ``yield`` in a generator: a ``for``
    loop does not throw the body's exception back into the generator, so a generator-only
    design would never see the deadlock it is supposed to recover from.
    """

    def __init__(self, state: _ReplayState, number: int, session: Session) -> None:
        self._state = state
        self._session = session
        self.number = number

    def __enter__(self) -> _Attempt:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is None:
            self._state.succeeded = True
            return False

        # Roll back before anything else: on MySQL the connection is already outside a
        # transaction, and on PostgreSQL it sits in a failed one where every statement errors.
        try:
            self._session.rollback()
        except Exception:
            log.debug("rollback after attempt %d failed; the transaction was already gone", self.number)
        # Drop every identity-mapped object, so the next attempt re-reads its state instead of
        # reusing instances populated by a transaction that no longer happened.
        try:
            self._session.expunge_all()
        except Exception:
            log.debug("expunge_all after attempt %d failed", self.number)

        if not is_deadlock(exc):
            return False
        self._state.last = exc
        if self.number >= self._state.max_attempts:
            self._state.exhausted = True
        return True


def replay_on_deadlock(
    session: Session,
    *,
    max_attempts: int = MAX_ATTEMPTS,
    description: str = "transaction",
) -> Iterator[_Attempt]:
    """
    Yield attempts, replaying the whole block when it loses a deadlock.

    Usage::

        for attempt in replay_on_deadlock(session, description="create_asset_event"):
            with attempt:
                do_all_the_database_work()
                session.commit()

    The guarded block must contain **everything the rollback discards**, including its commit,
    and must rebuild any per-attempt state it accumulates. Side effects that cannot be undone
    -- listeners, callbacks, external calls -- must be deferred until after the commit that
    survived, because every earlier attempt is erased.

    Anything that is not a deadlock propagates immediately, after the rollback. On exhaustion
    the *original* deadlock is raised rather than whatever cleanup error masked it.
    """
    state = _ReplayState(max_attempts)
    for number in range(1, max_attempts + 1):
        yield _Attempt(state, number, session)
        if state.succeeded:
            return
        if state.exhausted:
            break
        if state.last is None:
            # The body neither succeeded nor failed -- it must have broken out of the loop.
            return
        delay = BACKOFF_SECONDS[min(number - 1, len(BACKOFF_SECONDS) - 1)]
        log.warning(
            "Deadlock on %s; replaying the transaction (attempt %d of %d)",
            description,
            number,
            max_attempts,
            exc_info=state.last,
        )
        time.sleep(delay)
    if state.last is not None:
        log.error("Deadlock on %s persisted after %d attempts", description, max_attempts)
        raise original_deadlock(state.last)
