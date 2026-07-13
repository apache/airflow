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
from collections.abc import Generator
from typing import TYPE_CHECKING, Any

from sqlalchemy import TIMESTAMP, PickleType, String, event, nullsfirst, text
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ColumnElement
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.types import JSON, NullType, Text, TypeDecorator

from airflow._shared.timezones.timezone import make_naive, utc
from airflow.configuration import conf
from airflow.serialization.enums import Encoding

if TYPE_CHECKING:
    from collections.abc import Iterable

    from kubernetes.client.models.v1_pod import V1Pod
    from sqlalchemy.dialects.mysql.dml import Insert as MySQLInsert
    from sqlalchemy.dialects.postgresql.dml import Insert as PostgreSQLInsert
    from sqlalchemy.dialects.sqlite.dml import Insert as SQLiteInsert
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select
    from sqlalchemy.types import TypeEngine

    from airflow.typing_compat import Self


log = logging.getLogger(__name__)


def get_dialect_name(session: Session) -> str | None:
    """Safely get the name of the dialect associated with the given session."""
    if (bind := session.get_bind()) is None:
        raise ValueError("No bind/engine is associated with the provided Session")
    return getattr(bind.dialect, "name", None)


def build_upsert_stmt(
    dialect: str | None,
    model: Any,
    conflict_cols: list[str],
    values: dict[str, Any],
    update_fields: dict[str, Any],
) -> MySQLInsert | PostgreSQLInsert | SQLiteInsert:
    """
    Build a dialect-specific ``INSERT ... ON CONFLICT DO UPDATE`` (upsert) statement.

    A single-statement upsert is atomic at the database level, which avoids the
    race conditions that arise from the non-atomic SELECT-then-INSERT performed by
    ``session.merge()`` when concurrent transactions target the same primary key.

    :param dialect: dialect name as returned by :func:`get_dialect_name`
    :param model: the SQLAlchemy model (or table) to insert into
    :param conflict_cols: columns that make up the conflict target (PostgreSQL/SQLite)
    :param values: column values to insert
    :param update_fields: column values to set when a conflicting row already exists
    :raises ValueError: if the dialect does not support a known upsert syntax
    """
    stmt: MySQLInsert | PostgreSQLInsert | SQLiteInsert
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = pg_insert(model).values(**values)
        stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_fields)
    elif dialect == "mysql":
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        stmt = mysql_insert(model).values(**values)
        stmt = stmt.on_duplicate_key_update(**update_fields)
    elif dialect == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        stmt = sqlite_insert(model).values(**values)
        stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_fields)
    else:
        raise ValueError(
            f"Unsupported database dialect '{dialect}' for upsert. "
            "Supported dialects are: postgresql, mysql, sqlite."
        )
    return stmt


class random_db_uuid(FunctionElement):
    """
    Cross-dialect random UUID generation for use in SQL expressions.

    Compiles to ``gen_random_uuid()`` on PostgreSQL, ``UUID()`` on MySQL,
    and ``uuid4()`` on SQLite (registered via :func:`setup_event_handlers`).
    """

    type = String()
    inherit_cache = True


@compiles(random_db_uuid, "postgresql")
def _random_db_uuid_pg(element, compiler, **kw):
    return "gen_random_uuid()"


@compiles(random_db_uuid, "mysql")
def _random_db_uuid_mysql(element, compiler, **kw):
    return "UUID()"


@compiles(random_db_uuid, "sqlite")
def _random_db_uuid_sqlite(element, compiler, **kw):
    return "uuid4()"


class JsonContains(ColumnElement):
    """
    Dialect-aware JSON containment check.

    Compiles to ``@>`` on PostgreSQL (GIN-indexable), ``JSON_CONTAINS`` on
    MySQL, and per-key ``json_extract`` comparisons on SQLite.

    All dialects use bound parameters to avoid SQL injection.
    """

    inherit_cache = False
    type = NullType()

    def __init__(self, column, kv_dict: dict[str, str]):
        self.column = column
        self.kv_dict = kv_dict


@compiles(JsonContains, "postgresql")
def _pg_json_contains(element, compiler, **kw):
    from sqlalchemy import cast, literal

    col = cast(element.column, JSONB)
    param = literal(json.dumps(element.kv_dict)).cast(JSONB)
    expr = col.contains(param)
    return compiler.process(expr, **kw)


@compiles(JsonContains, "mysql")
def _mysql_json_contains(element, compiler, **kw):
    from sqlalchemy import bindparam, func

    param = bindparam(None, json.dumps(element.kv_dict), expanding=False)
    expr = func.JSON_CONTAINS(element.column, param)
    return compiler.process(expr == 1, **kw)


@compiles(JsonContains)
def _default_json_contains(element, compiler, **kw):
    from sqlalchemy import and_, func, literal

    clauses = []
    for k, v in element.kv_dict.items():
        path = f"$.{k}"
        clauses.append(func.json_extract(element.column, literal(path)) == literal(v))
    if len(clauses) == 1:
        return compiler.process(clauses[0], **kw)
    return compiler.process(and_(*clauses), **kw)


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


def deserialize_pod_dict(pod_dict: dict) -> V1Pod:
    """
    Deserialize a serialized pod dict back into a ``V1Pod``.

    kubernetes-client exposes no public dict->model API; see
    https://github.com/kubernetes-client/python/issues/977.

    A fresh ``Configuration`` is passed so that neither the pod nor any nested model captures the
    process-global in-cluster ``Configuration``. In-cluster, that global carries a
    ``refresh_api_key_hook`` local closure which ``pickle`` cannot serialize, and which would
    otherwise break pickling a ``pod_override`` onto the KubernetesExecutor multiprocessing queue.

    :meta private:
    """
    from kubernetes.client import Configuration
    from kubernetes.client.api_client import ApiClient
    from kubernetes.client.models.v1_pod import V1Pod

    return ApiClient(configuration=Configuration())._ApiClient__deserialize_model(pod_dict, V1Pod)


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
        # now we actually reserialize / deserialize the pod
        pod_dict = sanitize_for_serialization(pod)
        return deserialize_pod_dict(pod_dict)
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
    query: Select,
    session: Session,
    *,
    nowait: bool = False,
    skip_locked: bool = False,
    key_share: bool = True,
    **kwargs,
) -> Select:
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


@contextlib.contextmanager
def with_db_lock_timeout(session: Session, lock_timeout: int = 30) -> Generator[None, None, None]:
    """
    Context manager to set the database lock timeout for the current session.

    This prevents long-running operations from blocking indefinitely if they encounter
    lock contention. Only supported on PostgreSQL and MySQL.

    :param session: ORM Session
    :param lock_timeout: Lock timeout in seconds.
    """
    if lock_timeout <= 0:
        raise ValueError("lock_timeout must be a positive integer number of seconds")

    try:
        dialect_name = get_dialect_name(session)
    except ValueError:
        dialect_name = None

    old_mysql_timeout = None

    if dialect_name == "postgresql":
        # SET LOCAL applies only to the current transaction and resets on COMMIT/ROLLBACK.
        session.execute(text(f"SET LOCAL lock_timeout = '{lock_timeout}s'"))
    elif dialect_name == "mysql":
        old_mysql_timeout = session.execute(text("SELECT @@SESSION.innodb_lock_wait_timeout")).scalar()
        session.execute(text(f"SET SESSION innodb_lock_wait_timeout = {lock_timeout}"))
    else:
        log.debug(
            "Database lock timeout is not supported for dialect '%s'. "
            "The requested timeout of %ss will not be applied.",
            dialect_name,
            lock_timeout,
        )

    try:
        yield
    finally:
        if dialect_name == "mysql" and old_mysql_timeout is not None:
            session.execute(text(f"SET SESSION innodb_lock_wait_timeout = {old_mysql_timeout}"))


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
    # SQLite: `database is locked` (SQLITE_BUSY) — check the error text since
    # sqlite3.OperationalError.args[0] is a human-readable string, not a numeric code
    if error.orig and "database is locked" in str(error.orig).lower():
        return True
    return False


def make_dialect_kwarg(dialect: str) -> dict[str, str | Iterable[str]]:
    """Create an SQLAlchemy-version-aware dialect keyword argument."""
    return {"dialect_names": (dialect,)}
