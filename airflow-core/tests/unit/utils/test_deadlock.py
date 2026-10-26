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
from sqlalchemy.exc import IntegrityError, OperationalError

from airflow.utils.retries import (
    is_deadlock,
    original_deadlock,
    replay_on_deadlock,
)


class _Psycopg3Error(Exception):
    """psycopg3 surfaces the SQLSTATE as ``sqlstate``."""

    def __init__(self, sqlstate):
        super().__init__(f"psycopg3 error {sqlstate}")
        self.sqlstate = sqlstate


class _Psycopg2Error(Exception):
    """psycopg2 surfaces it as ``pgcode``."""

    def __init__(self, pgcode):
        super().__init__(f"psycopg2 error {pgcode}")
        self.pgcode = pgcode


class _MySQLError(Exception):
    """MySQL drivers put the errno first in ``args``."""

    def __init__(self, errno, message):
        super().__init__(errno, message)


def _wrap(orig):
    return OperationalError("SELECT 1", {}, orig)


class _FakeSession:
    def __init__(self):
        self.rollbacks = 0
        self.expunges = 0

    def rollback(self):
        self.rollbacks += 1

    def expunge_all(self):
        self.expunges += 1


class TestIsDeadlock:
    @pytest.mark.parametrize(
        "orig",
        [
            pytest.param(_Psycopg3Error("40P01"), id="psycopg3-sqlstate"),
            pytest.param(_Psycopg2Error("40P01"), id="psycopg2-pgcode"),
            pytest.param(_MySQLError(1213, "Deadlock found when trying to get lock"), id="mysql-1213"),
        ],
    )
    def test_recognises_each_driver(self, orig):
        assert is_deadlock(_wrap(orig)) is True

    @pytest.mark.parametrize(
        "orig",
        [
            pytest.param(_Psycopg3Error("40001"), id="serialization-failure"),
            pytest.param(_Psycopg3Error("23505"), id="unique-violation"),
            pytest.param(_MySQLError(1205, "Lock wait timeout exceeded"), id="mysql-lock-wait-timeout"),
            pytest.param(_MySQLError(1062, "Duplicate entry"), id="mysql-duplicate"),
        ],
    )
    def test_rejects_other_database_errors(self, orig):
        """Only a deadlock is replayable.

        A lock-wait timeout is deliberately excluded: InnoDB rolls back only the statement,
        so the transaction is still alive and a blind replay would double its work.
        """
        assert is_deadlock(_wrap(orig)) is False

    def test_bare_missing_savepoint_is_not_retryable(self):
        """1305 on its own means a SAVEPOINT was absent, which replay cannot fix."""
        assert is_deadlock(_wrap(_MySQLError(1305, "SAVEPOINT sa_savepoint_1 does not exist"))) is False

    def test_deadlock_masked_by_missing_savepoint_is_retryable(self):
        """The case that matters on MySQL.

        InnoDB rolls the victim's whole transaction back, so the ``RELEASE SAVEPOINT`` issued
        while unwinding fails with 1305 and becomes the exception the caller sees. The real
        1213 survives only in the cause chain.
        """
        deadlock = _wrap(_MySQLError(1213, "Deadlock found when trying to get lock"))
        masking = _wrap(_MySQLError(1305, "SAVEPOINT sa_savepoint_1 does not exist"))
        masking.__cause__ = deadlock
        assert is_deadlock(masking) is True
        assert original_deadlock(masking) is deadlock

    def test_follows_implicit_context_too(self):
        deadlock = _wrap(_Psycopg3Error("40P01"))
        masking = _wrap(_MySQLError(1305, "gone"))
        masking.__context__ = deadlock
        assert is_deadlock(masking) is True

    def test_non_database_error_is_not_a_deadlock(self):
        assert is_deadlock(RuntimeError("database adapter unavailable")) is False

    def test_cycle_in_the_cause_chain_terminates(self):
        a = _wrap(_MySQLError(1305, "a"))
        b = _wrap(_MySQLError(1305, "b"))
        a.__cause__ = b
        b.__cause__ = a
        assert is_deadlock(a) is False

    def test_original_deadlock_returns_the_input_when_there_is_none(self):
        err = _wrap(_MySQLError(1062, "Duplicate entry"))
        assert original_deadlock(err) is err


class TestReplayOnDeadlock:
    def test_commits_on_the_first_attempt_when_nothing_fails(self):
        session = _FakeSession()
        runs = 0
        for attempt in replay_on_deadlock(session):
            with attempt:
                runs += 1
        assert runs == 1
        assert session.rollbacks == 0

    def test_replays_until_it_succeeds(self):
        session = _FakeSession()
        runs = 0
        for attempt in replay_on_deadlock(session):
            with attempt:
                runs += 1
                if runs < 3:
                    raise _wrap(_Psycopg3Error("40P01"))
        assert runs == 3
        # Each failed attempt must leave a clean session for the next one.
        assert session.rollbacks == 2
        assert session.expunges == 2

    def test_attempts_are_bounded_and_raise_the_original_diagnostic(self):
        session = _FakeSession()
        runs = 0
        deadlock = _wrap(_MySQLError(1213, "Deadlock found when trying to get lock"))

        def always_deadlocks():
            nonlocal runs
            for attempt in replay_on_deadlock(session, max_attempts=3):
                with attempt:
                    runs += 1
                    masking = _wrap(_MySQLError(1305, "SAVEPOINT does not exist"))
                    masking.__cause__ = deadlock
                    raise masking

        with pytest.raises(OperationalError) as caught:
            always_deadlocks()

        assert runs == 3, "the attempt limit was not honoured"
        # Exhaustion must surface the deadlock, not the cleanup error that masked it.
        assert caught.value is deadlock
        assert caught.value.orig.args[0] == 1213

    def test_an_unrelated_error_propagates_without_replay(self):
        session = _FakeSession()
        runs = 0

        def violates_a_constraint():
            nonlocal runs
            for attempt in replay_on_deadlock(session):
                with attempt:
                    runs += 1
                    raise IntegrityError("INSERT", {}, _Psycopg3Error("23505"))

        with pytest.raises(IntegrityError):
            violates_a_constraint()
        assert runs == 1, "a constraint violation must not be replayed"
        # It still has to leave the session usable for the caller's own error handling.
        assert session.rollbacks == 1

    def test_non_database_errors_propagate(self):
        session = _FakeSession()
        runs = 0

        def blows_up():
            nonlocal runs
            for attempt in replay_on_deadlock(session):
                with attempt:
                    runs += 1
                    raise RuntimeError("adapter went away")

        with pytest.raises(RuntimeError, match="adapter"):
            blows_up()
        assert runs == 1

    def test_breaking_out_of_the_loop_does_not_raise(self):
        session = _FakeSession()
        for attempt in replay_on_deadlock(session):
            with attempt:
                pass
            break
        assert session.rollbacks == 0
