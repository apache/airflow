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

import datetime
import json
import logging
from typing import Any, Dict, Iterable, Tuple

import pendulum
from dateutil import relativedelta
from sqlalchemy import TIMESTAMP, PickleType, and_, event, false, nullsfirst, or_, tuple_
from sqlalchemy.dialects import mssql, mysql
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql.expression import ColumnOperators
from sqlalchemy.types import JSON, Text, TypeDecorator, TypeEngine, UnicodeText

from airflow import settings
from airflow.configuration import conf
from airflow.serialization.enums import Encoding

log = logging.getLogger(__name__)

utc = pendulum.tz.timezone('UTC')

using_mysql = conf.get_mandatory_value('database', 'sql_alchemy_conn').lower().startswith('mysql')


class UtcDateTime(TypeDecorator):
    """
    Almost equivalent to :class:`~sqlalchemy.types.TIMESTAMP` with
    ``timezone=True`` option, but it differs from that by:

    - Never silently take naive :class:`~datetime.datetime`, instead it
      always raise :exc:`ValueError` unless time zone aware value.
    - :class:`~datetime.datetime` value's :attr:`~datetime.datetime.tzinfo`
      is always converted to UTC.
    - Unlike SQLAlchemy's built-in :class:`~sqlalchemy.types.TIMESTAMP`,
      it never return naive :class:`~datetime.datetime`, but time zone
      aware value, even with SQLite or MySQL.
    - Always returns TIMESTAMP in UTC

    """

    impl = TIMESTAMP(timezone=True)

    def process_bind_param(self, value, dialect):
        if value is not None:
            if not isinstance(value, datetime.datetime):
                raise TypeError('expected datetime.datetime, not ' + repr(value))
            elif value.tzinfo is None:
                raise ValueError('naive datetime is disallowed')
            # For mysql we should store timestamps as naive values
            # Timestamp in MYSQL is not timezone aware. In MySQL 5.6
            # timezone added at the end is ignored but in MySQL 5.7
            # inserting timezone value fails with 'invalid-date'
            # See https://issues.apache.org/jira/browse/AIRFLOW-7001
            if using_mysql:
                from airflow.utils.timezone import make_naive

                return make_naive(value, timezone=utc)
            return value.astimezone(utc)
        return None

    def process_result_value(self, value, dialect):
        """
        Processes DateTimes from the DB making sure it is always
        returning UTC. Not using timezone.convert_to_utc as that
        converts to configured TIMEZONE while the DB might be
        running with some other setting. We assume UTC datetimes
        in the database.
        """
        if value is not None:
            if value.tzinfo is None:
                value = value.replace(tzinfo=utc)
            else:
                value = value.astimezone(utc)

        return value

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mssql':
            return mssql.DATETIME2(precision=6)
        elif dialect.name == 'mysql':
            return mysql.TIMESTAMP(fsp=6)
        return super().load_dialect_impl(dialect)


class ExtendedJSON(TypeDecorator):
    """
    A version of the JSON column that uses the Airflow extended JSON
    serialization provided by airflow.serialization.
    """

    impl = Text

    def db_supports_json(self):
        """Checks if the database supports JSON (i.e. is NOT MSSQL)"""
        return not conf.get("database", "sql_alchemy_conn").startswith("mssql")

    def load_dialect_impl(self, dialect) -> "TypeEngine":
        if self.db_supports_json():
            return dialect.type_descriptor(JSON)
        return dialect.type_descriptor(UnicodeText)

    def process_bind_param(self, value, dialect):
        from airflow.serialization.serialized_objects import BaseSerialization

        if value is None:
            return None

        # First, encode it into our custom JSON-targeted dict format
        value = BaseSerialization._serialize(value)

        # Then, if the database does not have native JSON support, encode it again as a string
        if not self.db_supports_json():
            value = json.dumps(value)

        return value

    def process_result_value(self, value, dialect):
        from airflow.serialization.serialized_objects import BaseSerialization

        if value is None:
            return None

        # Deserialize from a string first if needed
        if not self.db_supports_json():
            value = json.loads(value)

        return BaseSerialization._deserialize(value)


class ExecutorConfigType(PickleType):
    """
    Adds special handling for K8s executor config. If we unpickle a k8s object that was
    pickled under an earlier k8s library version, then the unpickled object may throw an error
    when to_dict is called.  To be more tolerant of version changes we convert to JSON using
    Airflow's serializer before pickling.
    """

    def bind_processor(self, dialect):

        from airflow.serialization.serialized_objects import BaseSerialization

        super_process = super().bind_processor(dialect)

        def process(value):
            if isinstance(value, dict) and 'pod_override' in value:
                value['pod_override'] = BaseSerialization()._serialize(value['pod_override'])
            return super_process(value)

        return process

    def result_processor(self, dialect, coltype):
        from airflow.serialization.serialized_objects import BaseSerialization

        super_process = super().result_processor(dialect, coltype)

        def process(value):
            value = super_process(value)  # unpickle

            if isinstance(value, dict) and 'pod_override' in value:
                pod_override = value['pod_override']

                # If pod_override was serialized with Airflow's BaseSerialization, deserialize it
                if isinstance(pod_override, dict) and pod_override.get(Encoding.TYPE):
                    value['pod_override'] = BaseSerialization()._deserialize(pod_override)
            return value

        return process

    def compare_values(self, x, y):
        """
        The TaskInstance.executor_config attribute is a pickled object that may contain
        kubernetes objects.  If the installed library version has changed since the
        object was originally pickled, due to the underlying ``__eq__`` method on these
        objects (which converts them to JSON), we may encounter attribute errors. In this
        case we should replace the stored object.

        From https://github.com/apache/airflow/pull/24356 we use our serializer to store
        k8s objects, but there could still be raw pickled k8s objects in the database,
        stored from earlier version, so we still compare them defensively here.
        """
        if self.comparator:
            return self.comparator(x, y)
        else:
            try:
                return x == y
            except AttributeError:
                return False


class Interval(TypeDecorator):
    """Base class representing a time interval."""

    impl = Text

    attr_keys = {
        datetime.timedelta: ('days', 'seconds', 'microseconds'),
        relativedelta.relativedelta: (
            'years',
            'months',
            'days',
            'leapdays',
            'hours',
            'minutes',
            'seconds',
            'microseconds',
            'year',
            'month',
            'day',
            'hour',
            'minute',
            'second',
            'microsecond',
        ),
    }

    def process_bind_param(self, value, dialect):
        if isinstance(value, tuple(self.attr_keys)):
            attrs = {key: getattr(value, key) for key in self.attr_keys[type(value)]}
            return json.dumps({'type': type(value).__name__, 'attrs': attrs})
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        if not value:
            return value
        data = json.loads(value)
        if isinstance(data, dict):
            type_map = {key.__name__: key for key in self.attr_keys}
            return type_map[data['type']](**data['attrs'])
        return data


def skip_locked(session: Session) -> Dict[str, Any]:
    """
    Return kargs for passing to `with_for_update()` suitable for the current DB engine version.

    We do this as we document the fact that on DB engines that don't support this construct, we do not
    support/recommend running HA scheduler. If a user ignores this and tries anyway everything will still
    work, just slightly slower in some circumstances.

    Specifically don't emit SKIP LOCKED for MySQL < 8, or MariaDB, neither of which support this construct

    See https://jira.mariadb.org/browse/MDEV-13115
    """
    dialect = session.bind.dialect

    if dialect.name != "mysql" or dialect.supports_for_update_of:
        return {'skip_locked': True}
    else:
        return {}


def nowait(session: Session) -> Dict[str, Any]:
    """
    Return kwargs for passing to `with_for_update()` suitable for the current DB engine version.

    We do this as we document the fact that on DB engines that don't support this construct, we do not
    support/recommend running HA scheduler. If a user ignores this and tries anyway everything will still
    work, just slightly slower in some circumstances.

    Specifically don't emit NOWAIT for MySQL < 8, or MariaDB, neither of which support this construct

    See https://jira.mariadb.org/browse/MDEV-13115
    """
    dialect = session.bind.dialect

    if dialect.name != "mysql" or dialect.supports_for_update_of:
        return {'nowait': True}
    else:
        return {}


def nulls_first(col, session: Session) -> Dict[str, Any]:
    """
    Adds a nullsfirst construct to the column ordering. Currently only Postgres supports it.
    In MySQL & Sqlite NULL values are considered lower than any non-NULL value, therefore, NULL values
    appear first when the order is ASC (ascending)
    """
    if session.bind.dialect.name == "postgresql":
        return nullsfirst(col)
    else:
        return col


USE_ROW_LEVEL_LOCKING: bool = conf.getboolean('scheduler', 'use_row_level_locking', fallback=True)


def with_row_locks(query, session: Session, **kwargs):
    """
    Apply with_for_update to an SQLAlchemy query, if row level locking is in use.

    :param query: An SQLAlchemy Query object
    :param session: ORM Session
    :param kwargs: Extra kwargs to pass to with_for_update (of, nowait, skip_locked, etc)
    :return: updated query
    """
    dialect = session.bind.dialect

    # Don't use row level locks if the MySQL dialect (Mariadb & MySQL < 8) does not support it.
    if USE_ROW_LEVEL_LOCKING and (dialect.name != "mysql" or dialect.supports_for_update_of):
        return query.with_for_update(**kwargs)
    else:
        return query


class CommitProhibitorGuard:
    """Context manager class that powers prohibit_commit"""

    expected_commit = False

    def __init__(self, session: Session):
        self.session = session

    def _validate_commit(self, _):
        if self.expected_commit:
            self.expected_commit = False
            return
        raise RuntimeError("UNEXPECTED COMMIT - THIS WILL BREAK HA LOCKS!")

    def __enter__(self):
        event.listen(self.session, 'before_commit', self._validate_commit)
        return self

    def __exit__(self, *exc_info):
        event.remove(self.session, 'before_commit', self._validate_commit)

    def commit(self):
        """
        Commit the session.

        This is the required way to commit when the guard is in scope
        """
        self.expected_commit = True
        self.session.commit()


def prohibit_commit(session):
    """
    Return a context manager that will disallow any commit that isn't done via the context manager.

    The aim of this is to ensure that transaction lifetime is strictly controlled which is especially
    important in the core scheduler loop. Any commit on the session that is _not_ via this context manager
    will result in RuntimeError

    Example usage:

    .. code:: python

        with prohibit_commit(session) as guard:
            # ... do something with session
            guard.commit()

            # This would throw an error
            # session.commit()
    """
    return CommitProhibitorGuard(session)


def is_lock_not_available_error(error: OperationalError):
    """Check if the Error is about not being able to acquire lock"""
    # DB specific error codes:
    # Postgres: 55P03
    # MySQL: 3572, 'Statement aborted because lock(s) could not be acquired immediately and NOWAIT
    #               is set.'
    # MySQL: 1205, 'Lock wait timeout exceeded; try restarting transaction
    #              (when NOWAIT isn't available)
    db_err_code = getattr(error.orig, 'pgcode', None) or error.orig.args[0]

    # We could test if error.orig is an instance of
    # psycopg2.errors.LockNotAvailable/_mysql_exceptions.OperationalError, but that involves
    # importing it. This doesn't
    if db_err_code in ('55P03', 1205, 3572):
        return True
    return False


def tuple_in_condition(
    columns: Tuple[ColumnElement, ...],
    collection: Iterable[Any],
) -> ColumnOperators:
    """Generates a tuple-in-collection operator to use in ``.filter()``.

    For most SQL backends, this generates a simple ``([col, ...]) IN [condition]``
    clause. This however does not work with MSSQL, where we need to expand to
    ``(c1 = v1a AND c2 = v2a ...) OR (c1 = v1b AND c2 = v2b ...) ...`` manually.

    :meta private:
    """
    if settings.engine.dialect.name != "mssql":
        return tuple_(*columns).in_(collection)
    clauses = [and_(*(c == v for c, v in zip(columns, values))) for values in collection]
    if not clauses:
        return false()
    return or_(*clauses)
