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

import contextlib
import copy
import datetime
import json
import logging
from typing import TYPE_CHECKING, Any, Generator, Iterable, overload

import pendulum
from dateutil import relativedelta
from sqlalchemy import TIMESTAMP, PickleType, and_, event, false, nullsfirst, or_, true, tuple_
from sqlalchemy.dialects import mssql, mysql
from sqlalchemy.sql import Select
from sqlalchemy.types import JSON, Text, TypeDecorator, UnicodeText

from airflow import settings
from airflow.configuration import conf
from airflow.serialization.enums import Encoding
from airflow.utils.timezone import make_naive

if TYPE_CHECKING:
    from kubernetes.client.models.v1_pod import V1Pod
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.orm import Query, Session
    from sqlalchemy.sql import ColumnElement
    from sqlalchemy.sql.expression import ColumnOperators
    from sqlalchemy.types import TypeEngine

log = logging.getLogger(__name__)

utc = pendulum.tz.timezone("UTC")


class UtcDateTime(TypeDecorator):
    """
    Similar to :class:`~sqlalchemy.types.TIMESTAMP` with ``timezone=True`` option, with some differences.

    - Never silently take naive :class:`~datetime.datetime`, instead it
      always raise :exc:`ValueError` unless time zone aware value.
    - :class:`~datetime.datetime` value's :attr:`~datetime.datetime.tzinfo`
      is always converted to UTC.
    - Unlike SQLAlchemy's built-in :class:`~sqlalchemy.types.TIMESTAMP`,
      it never return naive :class:`~datetime.datetime`, but time zone
      aware value, even with SQLite or MySQL.
    - Always returns TIMESTAMP in UTC.
    """

    impl = TIMESTAMP(timezone=True)

    cache_ok = True

    def process_bind_param(self, value, dialect):
        if not isinstance(value, datetime.datetime):
            if value is None:
                return None
            raise TypeError(f"expected datetime.datetime, not {value!r}")
        elif value.tzinfo is None:
            raise ValueError("naive datetime is disallowed")
        elif dialect.name == "mysql":
            # For mysql we should store timestamps as naive values
            # In MySQL 5.7 inserting timezone value fails with 'invalid-date'
            # See https://issues.apache.org/jira/browse/AIRFLOW-7001
            return make_naive(value, timezone=utc)
        return value.astimezone(utc)

    def process_result_value(self, value, dialect):
        """
        Process DateTimes from the DB making sure to always return UTC.

        Not using timezone.convert_to_utc as that converts to configured TIMEZONE
        while the DB might be running with some other setting. We assume UTC
        datetimes in the database.
        """
        if value is not None:
            if value.tzinfo is None:
                value = value.replace(tzinfo=utc)
            else:
                value = value.astimezone(utc)

        return value

    def load_dialect_impl(self, dialect):
        if dialect.name == "mssql":
            return mssql.DATETIME2(precision=6)
        elif dialect.name == "mysql":
            return mysql.TIMESTAMP(fsp=6)
        return super().load_dialect_impl(dialect)


class ExtendedJSON(TypeDecorator):
    """
    A version of the JSON column that uses the Airflow extended JSON serialization.

    See airflow.serialization.
    """

    impl = Text

    cache_ok = True

    def load_dialect_impl(self, dialect) -> TypeEngine:
        if dialect.name != "mssql":
            return dialect.type_descriptor(JSON)
        return dialect.type_descriptor(UnicodeText)

    def process_bind_param(self, value, dialect):
        from airflow.serialization.serialized_objects import BaseSerialization

        if value is None:
            return None

        # First, encode it into our custom JSON-targeted dict format
        value = BaseSerialization.serialize(value)

        # Then, if the database does not have native JSON support, encode it again as a string
        if dialect.name == "mssql":
            value = json.dumps(value)

        return value

    def process_result_value(self, value, dialect):
        from airflow.serialization.serialized_objects import BaseSerialization

        if value is None:
            return None

        # Deserialize from a string first if needed
        if dialect.name == "mssql":
            value = json.loads(value)

        return BaseSerialization.deserialize(value)


def sanitize_for_serialization(obj: V1Pod):
    """
    Convert pod to dict.... but *safely*.

    When pod objects created with one k8s version are unpickled in a python
    env with a more recent k8s version (in which the object attrs may have
    changed) the unpickled obj may throw an error because the attr
    expected on new obj may not be there on the unpickled obj.

    This function still converts the pod to a dict; the only difference is
    it populates missing attrs with None. You may compare with
    https://github.com/kubernetes-client/python/blob/5a96bbcbe21a552cc1f9cda13e0522fafb0dbac8/kubernetes/client/api_client.py#L202

    If obj is None, return None.
    If obj is str, int, long, float, bool, return directly.
    If obj is datetime.datetime, datetime.date
        convert to string in iso8601 format.
    If obj is list, sanitize each element in the list.
    If obj is dict, return the dict.
    If obj is OpenAPI model, return the properties dict.

    :param obj: The data to serialize.
    :return: The serialized form of data.

    :meta private:
    """
    if obj is None:
        return None
    elif isinstance(obj, (float, bool, bytes, str, int)):
        return obj
    elif isinstance(obj, list):
        return [sanitize_for_serialization(sub_obj) for sub_obj in obj]
    elif isinstance(obj, tuple):
        return tuple(sanitize_for_serialization(sub_obj) for sub_obj in obj)
    elif isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()

    if isinstance(obj, dict):
        obj_dict = obj
    else:
        obj_dict = {
            obj.attribute_map[attr]: getattr(obj, attr)
            for attr, _ in obj.openapi_types.items()
            # below is the only line we change, and we just add default=None for getattr
            if getattr(obj, attr, None) is not None
        }

    return {key: sanitize_for_serialization(val) for key, val in obj_dict.items()}


def ensure_pod_is_valid_after_unpickling(pod: V1Pod) -> V1Pod | None:
    """
    Convert pod to json and back so that pod is safe.

    The pod_override in executor_config is a V1Pod object.
    Such objects created with one k8s version, when unpickled in
    an env with upgraded k8s version, may blow up when
    `to_dict` is called, because openapi client code gen calls
    getattr on all attrs in openapi_types for each object, and when
    new attrs are added to that list, getattr will fail.

    Here we re-serialize it to ensure it is not going to blow up.

    :meta private:
    """
    try:
        # if to_dict works, the pod is fine
        pod.to_dict()
        return pod
    except AttributeError:
        pass
    try:
        from kubernetes.client.models.v1_pod import V1Pod
    except ImportError:
        return None
    if not isinstance(pod, V1Pod):
        return None
    try:
        try:
            from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
        except ImportError:
            from airflow.kubernetes.pre_7_4_0_compatibility.pod_generator import (  # type: ignore[assignment]
                PodGenerator,
            )
        # now we actually reserialize / deserialize the pod
        pod_dict = sanitize_for_serialization(pod)
        return PodGenerator.deserialize_model_dict(pod_dict)
    except Exception:
        return None


class ExecutorConfigType(PickleType):
    """
    Adds special handling for K8s executor config.

    If we unpickle a k8s object that was pickled under an earlier k8s library version, then
    the unpickled object may throw an error when to_dict is called.  To be more tolerant of
    version changes we convert to JSON using Airflow's serializer before pickling.
    """

    cache_ok = True

    def bind_processor(self, dialect):

        from airflow.serialization.serialized_objects import BaseSerialization

        super_process = super().bind_processor(dialect)

        def process(value):
            val_copy = copy.copy(value)
            if isinstance(val_copy, dict) and "pod_override" in val_copy:
                val_copy["pod_override"] = BaseSerialization.serialize(val_copy["pod_override"])
            return super_process(val_copy)

        return process

    def result_processor(self, dialect, coltype):
        from airflow.serialization.serialized_objects import BaseSerialization

        super_process = super().result_processor(dialect, coltype)

        def process(value):
            value = super_process(value)  # unpickle

            if isinstance(value, dict) and "pod_override" in value:
                pod_override = value["pod_override"]

                if isinstance(pod_override, dict) and pod_override.get(Encoding.TYPE):
                    # If pod_override was serialized with Airflow's BaseSerialization, deserialize it
                    value["pod_override"] = BaseSerialization.deserialize(pod_override)
                else:
                    # backcompat path
                    # we no longer pickle raw pods but this code may be reached
                    # when accessing executor configs created in a prior version
                    new_pod = ensure_pod_is_valid_after_unpickling(pod_override)
                    if new_pod:
                        value["pod_override"] = new_pod
            return value

        return process

    def compare_values(self, x, y):
        """
        The TaskInstance.executor_config attribute is a pickled object that may contain kubernetes objects.

        If the installed library version has changed since the object was originally pickled,
        due to the underlying ``__eq__`` method on these objects (which converts them to JSON),
        we may encounter attribute errors. In this case we should replace the stored object.

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

    cache_ok = True

    attr_keys = {
        datetime.timedelta: ("days", "seconds", "microseconds"),
        relativedelta.relativedelta: (
            "years",
            "months",
            "days",
            "leapdays",
            "hours",
            "minutes",
            "seconds",
            "microseconds",
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "second",
            "microsecond",
        ),
    }

    def process_bind_param(self, value, dialect):
        if isinstance(value, tuple(self.attr_keys)):
            attrs = {key: getattr(value, key) for key in self.attr_keys[type(value)]}
            return json.dumps({"type": type(value).__name__, "attrs": attrs})
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        if not value:
            return value
        data = json.loads(value)
        if isinstance(data, dict):
            type_map = {key.__name__: key for key in self.attr_keys}
            return type_map[data["type"]](**data["attrs"])
        return data


def skip_locked(session: Session) -> dict[str, Any]:
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
        return {"skip_locked": True}
    else:
        return {}


def nowait(session: Session) -> dict[str, Any]:
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
        return {"nowait": True}
    else:
        return {}


def nulls_first(col, session: Session) -> dict[str, Any]:
    """Specify *NULLS FIRST* to the column ordering.

    This is only done to Postgres, currently the only backend that supports it.
    Other databases do not need it since NULL values are considered lower than
    any other values, and appear first when the order is ASC (ascending).
    """
    if session.bind.dialect.name == "postgresql":
        return nullsfirst(col)
    else:
        return col


USE_ROW_LEVEL_LOCKING: bool = conf.getboolean("scheduler", "use_row_level_locking", fallback=True)


def with_row_locks(query: Query, session: Session, **kwargs) -> Query:
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


@contextlib.contextmanager
def lock_rows(query: Query, session: Session) -> Generator[None, None, None]:
    """Lock database rows during the context manager block.

    This is a convenient method for ``with_row_locks`` when we don't need the
    locked rows.

    :meta private:
    """
    locked_rows = with_row_locks(query, session).all()
    yield
    del locked_rows


class CommitProhibitorGuard:
    """Context manager class that powers prohibit_commit."""

    expected_commit = False

    def __init__(self, session: Session):
        self.session = session

    def _validate_commit(self, _):
        if self.expected_commit:
            self.expected_commit = False
            return
        raise RuntimeError("UNEXPECTED COMMIT - THIS WILL BREAK HA LOCKS!")

    def __enter__(self):
        event.listen(self.session, "before_commit", self._validate_commit)
        return self

    def __exit__(self, *exc_info):
        event.remove(self.session, "before_commit", self._validate_commit)

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
    """Check if the Error is about not being able to acquire lock."""
    # DB specific error codes:
    # Postgres: 55P03
    # MySQL: 3572, 'Statement aborted because lock(s) could not be acquired immediately and NOWAIT
    #               is set.'
    # MySQL: 1205, 'Lock wait timeout exceeded; try restarting transaction
    #              (when NOWAIT isn't available)
    db_err_code = getattr(error.orig, "pgcode", None) or error.orig.args[0]

    # We could test if error.orig is an instance of
    # psycopg2.errors.LockNotAvailable/_mysql_exceptions.OperationalError, but that involves
    # importing it. This doesn't
    if db_err_code in ("55P03", 1205, 3572):
        return True
    return False


@overload
def tuple_in_condition(
    columns: tuple[ColumnElement, ...],
    collection: Iterable[Any],
) -> ColumnOperators:
    ...


@overload
def tuple_in_condition(
    columns: tuple[ColumnElement, ...],
    collection: Select,
    *,
    session: Session,
) -> ColumnOperators:
    ...


def tuple_in_condition(
    columns: tuple[ColumnElement, ...],
    collection: Iterable[Any] | Select,
    *,
    session: Session | None = None,
) -> ColumnOperators:
    """
    Generate a tuple-in-collection operator to use in ``.where()``.

    For most SQL backends, this generates a simple ``([col, ...]) IN [condition]``
    clause. This however does not work with MSSQL, where we need to expand to
    ``(c1 = v1a AND c2 = v2a ...) OR (c1 = v1b AND c2 = v2b ...) ...`` manually.

    :meta private:
    """
    if settings.engine.dialect.name != "mssql":
        return tuple_(*columns).in_(collection)
    if not isinstance(collection, Select):
        rows = collection
    elif session is None:
        raise TypeError("session is required when passing in a subquery")
    else:
        rows = session.execute(collection)
    clauses = [and_(*(c == v for c, v in zip(columns, values))) for values in rows]
    if not clauses:
        return false()
    return or_(*clauses)


@overload
def tuple_not_in_condition(
    columns: tuple[ColumnElement, ...],
    collection: Iterable[Any],
) -> ColumnOperators:
    ...


@overload
def tuple_not_in_condition(
    columns: tuple[ColumnElement, ...],
    collection: Select,
    *,
    session: Session,
) -> ColumnOperators:
    ...


def tuple_not_in_condition(
    columns: tuple[ColumnElement, ...],
    collection: Iterable[Any] | Select,
    *,
    session: Session | None = None,
) -> ColumnOperators:
    """
    Generate a tuple-not-in-collection operator to use in ``.where()``.

    This is similar to ``tuple_in_condition`` except generating ``NOT IN``.

    :meta private:
    """
    if settings.engine.dialect.name != "mssql":
        return tuple_(*columns).not_in(collection)
    if not isinstance(collection, Select):
        rows = collection
    elif session is None:
        raise TypeError("session is required when passing in a subquery")
    else:
        rows = session.execute(collection)
    clauses = [or_(*(c != v for c, v in zip(columns, values))) for values in rows]
    if not clauses:
        return true()
    return and_(*clauses)
