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
import logging
from collections.abc import Generator
from importlib import metadata
from typing import TYPE_CHECKING, Any

from packaging import version
from sqlalchemy import TIMESTAMP, PickleType, event, nullsfirst
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import JSON, Text, TypeDecorator

from airflow._shared.timezones.timezone import make_naive, utc
from airflow.configuration import conf
from airflow.serialization.enums import Encoding

if TYPE_CHECKING:
    from collections.abc import Iterable

    from kubernetes.client.models.v1_pod import V1Pod
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select
    from sqlalchemy.sql.elements import ColumnElement
    from sqlalchemy.types import TypeEngine

    from airflow.typing_compat import Self


log = logging.getLogger(__name__)

try:
    from sqlalchemy.orm import mapped_column
except ImportError:
    # fallback for SQLAlchemy < 2.0
    def mapped_column(*args, **kwargs):  # type: ignore[misc]
        from sqlalchemy import Column

        return Column(*args, **kwargs)


def get_dialect_name(session: Session) -> str | None:
    """Safely get the name of the dialect associated with the given session."""
    if (bind := session.get_bind()) is None:
        raise ValueError("No bind/engine is associated with the provided Session")
    return getattr(bind.dialect, "name", None)


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
        if value.tzinfo is None:
            raise ValueError("naive datetime is disallowed")
        if dialect.name == "mysql":
            # For mysql versions prior 8.0.19 we should send timestamps as naive values in UTC
            # see: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html
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
        if dialect.name == "mysql":
            return mysql.TIMESTAMP(fsp=6)
        return super().load_dialect_impl(dialect)


class ExtendedJSON(TypeDecorator):
    """
    A version of the JSON column that uses the Airflow extended JSON serialization.

    See airflow.serialization.
    """

    impl = Text

    cache_ok = True

    should_evaluate_none = True

    def load_dialect_impl(self, dialect) -> TypeEngine:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB)
        return dialect.type_descriptor(JSON)

    def process_bind_param(self, value, dialect):
        from airflow.serialization.serialized_objects import BaseSerialization

        if value is None:
            return None

        return BaseSerialization.serialize(value)

    def process_result_value(self, value, dialect):
        from airflow.serialization.serialized_objects import BaseSerialization

        if value is None:
            return None

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
    if isinstance(obj, (float, bool, bytes, str, int)):
        return obj
    if isinstance(obj, list):
        return [sanitize_for_serialization(sub_obj) for sub_obj in obj]
    if isinstance(obj, tuple):
        return tuple(sanitize_for_serialization(sub_obj) for sub_obj in obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
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
        from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator

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
        Compare x and y using self.comparator if available. Else, use __eq__.

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
        try:
            return x == y
        except AttributeError:
            return False


def nulls_first(col: ColumnElement, session: Session) -> ColumnElement:
    """
    Specify *NULLS FIRST* to the column ordering.

    This is only done to Postgres, currently the only backend that supports it.
    Other databases do not need it since NULL values are considered lower than
    any other values, and appear first when the order is ASC (ascending).
    """
    if get_dialect_name(session) == "postgresql":
        return nullsfirst(col)
    return col


USE_ROW_LEVEL_LOCKING: bool = conf.getboolean("scheduler", "use_row_level_locking", fallback=True)


def with_row_locks(
    query: Select[Any],
    session: Session,
    *,
    nowait: bool = False,
    skip_locked: bool = False,
    key_share: bool = True,
    **kwargs,
) -> Select[Any]:
    """
    Apply with_for_update to the SQLAlchemy query if row level locking is in use.

    This wrapper is needed so we don't use the syntax on unsupported database
    engines. In particular, MySQL (prior to 8.0) and MariaDB do not support
    row locking, where we do not support nor recommend running HA scheduler. If
    a user ignores this and tries anyway, everything will still work, just
    slightly slower in some circumstances.

    See https://jira.mariadb.org/browse/MDEV-13115

    :param query: An SQLAlchemy Query object
    :param session: ORM Session
    :param nowait: If set to True, will pass NOWAIT to supported database backends.
    :param skip_locked: If set to True, will pass SKIP LOCKED to supported database backends.
    :param key_share: If true, will lock with FOR KEY SHARE UPDATE (at least on postgres).
    :param kwargs: Extra kwargs to pass to with_for_update (of, nowait, skip_locked, etc)
    :return: updated query
    """
    try:
        dialect_name = get_dialect_name(session)
    except ValueError:
        return query
    if not dialect_name:
        return query

    # Don't use row level locks if the MySQL dialect (Mariadb & MySQL < 8) does not support it.
    if not USE_ROW_LEVEL_LOCKING:
        return query
    if dialect_name == "mysql" and not getattr(
        session.bind.dialect if session.bind else None, "supports_for_update_of", False
    ):
        return query
    if nowait:
        kwargs["nowait"] = True
    if skip_locked:
        kwargs["skip_locked"] = True
    if key_share:
        kwargs["key_share"] = True
    return query.with_for_update(**kwargs)


@contextlib.contextmanager
def lock_rows(query: Select, session: Session) -> Generator[None, None, None]:
    """
    Lock database rows during the context manager block.

    This is a convenient method for ``with_row_locks`` when we don't need the
    locked rows.

    :meta private:
    """
    locked_rows = with_row_locks(query, session)
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

    def __enter__(self) -> Self:
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
    db_err_code = getattr(error.orig, "pgcode", None) or (
        error.orig.args[0] if error.orig and error.orig.args else None
    )

    # We could test if error.orig is an instance of
    # psycopg2.errors.LockNotAvailable/_mysql_exceptions.OperationalError, but that involves
    # importing it. This doesn't
    if db_err_code in ("55P03", 1205, 3572):
        return True
    return False


def get_orm_mapper():
    """Get the correct ORM mapper for the installed SQLAlchemy version."""
    import sqlalchemy.orm.mapper

    return sqlalchemy.orm.mapper if is_sqlalchemy_v1() else sqlalchemy.orm.Mapper


def is_sqlalchemy_v1() -> bool:
    return version.parse(metadata.version("sqlalchemy")).major == 1


def make_dialect_kwarg(dialect: str) -> dict[str, str | Iterable[str]]:
    """Create an SQLAlchemy-version-aware dialect keyword argument."""
    return {"dialect_name": dialect} if is_sqlalchemy_v1() else {"dialect_names": (dialect,)}
