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
import json
import logging
import sys
import warnings
from typing import TYPE_CHECKING, Any

from sqlalchemy import Boolean, ForeignKey, Integer, String, Text, delete, or_, select
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.orm import Mapped, declared_attr, reconstructor, synonym

from airflow._shared.secrets_masker import mask_secret
from airflow.configuration import conf, ensure_secrets_loaded
from airflow.models.base import ID_LEN, Base
from airflow.models.crypto import get_fernet
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.sqlalchemy import get_dialect_name, mapped_column

if TYPE_CHECKING:
    from sqlalchemy.dialects.mysql.dml import Insert as MySQLInsert
    from sqlalchemy.dialects.postgresql.dml import Insert as PostgreSQLInsert
    from sqlalchemy.dialects.sqlite.dml import Insert as SQLiteInsert
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)


class Variable(Base, LoggingMixin):
    """A generic way to store and retrieve arbitrary content or settings as a simple key/value store."""

    __tablename__ = "variable"
    __NO_DEFAULT_SENTINEL = object()

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(ID_LEN), unique=True)
    _val: Mapped[str] = mapped_column("val", Text().with_variant(MEDIUMTEXT, "mysql"))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_encrypted: Mapped[bool] = mapped_column(Boolean, unique=False, default=False)
    team_name: Mapped[str | None] = mapped_column(
        String(50),
        ForeignKey("team.name", ondelete="SET NULL"),
        nullable=True,
    )

    def __init__(self, key=None, val=None, description=None, team_name=None):
        super().__init__()
        self.key = key
        self.val = val
        self.description = description
        self.team_name = team_name

    @reconstructor
    def on_db_load(self):
        if self._val:
            mask_secret(self.val, self.key)

    def __repr__(self):
        # Hiding the value
        return f"{self.key} : {self._val}"

    def get_val(self):
        """Get Airflow Variable from Metadata DB and decode it using the Fernet Key."""
        from cryptography.fernet import InvalidToken as InvalidFernetToken

        if self._val is not None and self.is_encrypted:
            try:
                fernet = get_fernet()
                return fernet.decrypt(bytes(self._val, "utf-8")).decode()
            except InvalidFernetToken:
                self.log.error("Can't decrypt _val for key=%s, invalid token or value", self.key)
                return None
            except Exception:
                self.log.error("Can't decrypt _val for key=%s, FERNET_KEY configuration missing", self.key)
                return None
        else:
            return self._val

    def set_val(self, value):
        """Encode the specified value with Fernet Key and store it in Variables Table."""
        if value is not None:
            fernet = get_fernet()
            self._val = fernet.encrypt(bytes(value, "utf-8")).decode()
            self.is_encrypted = fernet.is_encrypted

    @declared_attr
    def val(cls):
        """Get Airflow Variable from Metadata DB and decode it using the Fernet Key."""
        return synonym("_val", descriptor=property(cls.get_val, cls.set_val))

    @classmethod
    def setdefault(cls, key, default, description=None, deserialize_json=False):
        """
        Return the current value for a key or store the default value and return it.

        Works the same as the Python builtin dict object.

        :param key: Dict key for this Variable
        :param default: Default value to set and return if the variable
            isn't already in the DB
        :param description: Default value to set Description of the Variable
        :param deserialize_json: Store this as a JSON encoded value in the DB
            and un-encode it when retrieving a value
        :param session: Session
        :return: Mixed
        """
        obj = Variable.get(key, default_var=None, deserialize_json=deserialize_json)
        if obj is None:
            if default is not None:
                Variable.set(key=key, value=default, description=description, serialize_json=deserialize_json)
                return default
            raise ValueError("Default Value must be set")
        return obj

    @classmethod
    def get(
        cls,
        key: str,
        default_var: Any = __NO_DEFAULT_SENTINEL,
        deserialize_json: bool = False,
        team_name: str | None = None,
    ) -> Any:
        """
        Get a value for an Airflow Variable Key.

        :param key: Variable Key
        :param default_var: Default value of the Variable if the Variable doesn't exist
        :param deserialize_json: Deserialize the value to a Python dict
        :param team_name: Team name associated to the task trying to access the variable (if any)
        """
        # TODO: This is not the best way of having compat, but it's "better than erroring" for now. This still
        # means SQLA etc is loaded, but we can't avoid that unless/until we add import shims as a big
        # back-compat layer

        # If this is set it means we are in some kind of execution context (Task, Dag Parse or Triggerer perhaps)
        # and should use the Task SDK API server path
        if hasattr(sys.modules.get("airflow.sdk.execution_time.task_runner"), "SUPERVISOR_COMMS"):
            warnings.warn(
                "Using Variable.get from `airflow.models` is deprecated."
                "Please use `get` on Variable from sdk(`airflow.sdk.Variable`) instead",
                DeprecationWarning,
                stacklevel=1,
            )
            from airflow.sdk import Variable as TaskSDKVariable

            default_kwargs = {} if default_var is cls.__NO_DEFAULT_SENTINEL else {"default": default_var}
            var_val = TaskSDKVariable.get(key, deserialize_json=deserialize_json, **default_kwargs)
            if isinstance(var_val, str):
                mask_secret(var_val, key)

            return var_val

        if team_name and not conf.getboolean("core", "multi_team"):
            raise ValueError(
                "Multi-team mode is not configured in the Airflow environment but the task trying to access the variable belongs to a team"
            )

        var_val = Variable.get_variable_from_secrets(key=key, team_name=team_name)
        if var_val is None:
            if default_var is not cls.__NO_DEFAULT_SENTINEL:
                return default_var
            raise KeyError(f"Variable {key} does not exist")
        if deserialize_json:
            obj = json.loads(var_val)
            mask_secret(obj, key)
            return obj
        mask_secret(var_val, key)
        return var_val

    @staticmethod
    def set(
        key: str,
        value: Any,
        description: str | None = None,
        serialize_json: bool = False,
        team_name: str | None = None,
        session: Session | None = None,
    ) -> None:
        """
        Set a value for an Airflow Variable with a given Key.

        This operation overwrites an existing variable using the session's dialect-specific upsert operation.

        :param key: Variable Key
        :param value: Value to set for the Variable
        :param description: Description of the Variable
        :param serialize_json: Serialize the value to a JSON string
        :param team_name: Team name associated to the variable (if any)
        :param session: optional session, use if provided or create a new one
        """
        # TODO: This is not the best way of having compat, but it's "better than erroring" for now. This still
        # means SQLA etc is loaded, but we can't avoid that unless/until we add import shims as a big
        # back-compat layer

        # If this is set it means we are in some kind of execution context (Task, Dag Parse or Triggerer perhaps)
        # and should use the Task SDK API server path
        if hasattr(sys.modules.get("airflow.sdk.execution_time.task_runner"), "SUPERVISOR_COMMS"):
            warnings.warn(
                "Using Variable.set from `airflow.models` is deprecated."
                "Please use `set` on Variable from sdk(`airflow.sdk.Variable`) instead",
                DeprecationWarning,
                stacklevel=1,
            )
            from airflow.sdk import Variable as TaskSDKVariable

            TaskSDKVariable.set(
                key=key,
                value=value,
                description=description,
                serialize_json=serialize_json,
            )
            return

        if team_name and not conf.getboolean("core", "multi_team"):
            raise ValueError(
                "Multi-team mode is not configured in the Airflow environment. To assign a team to a variable, multi-mode must be enabled."
            )

        # check if the secret exists in the custom secrets' backend.
        from airflow.sdk import SecretCache

        Variable.check_for_write_conflict(key=key)
        if serialize_json:
            stored_value = json.dumps(value, indent=2)
        else:
            stored_value = str(value)

        ctx: contextlib.AbstractContextManager
        if session is not None:
            ctx = contextlib.nullcontext(session)
        else:
            ctx = create_session()

        with ctx as session:
            new_variable = Variable(key=key, val=stored_value, description=description, team_name=team_name)

            val = new_variable._val
            is_encrypted = new_variable.is_encrypted

            # Create dialect-specific upsert statement
            dialect_name = get_dialect_name(session)
            stmt: MySQLInsert | PostgreSQLInsert | SQLiteInsert

            if dialect_name == "postgresql":
                from sqlalchemy.dialects.postgresql import insert as pg_insert

                pg_stmt = pg_insert(Variable).values(
                    key=key,
                    val=val,
                    description=description,
                    is_encrypted=is_encrypted,
                    team_name=team_name,
                )
                stmt = pg_stmt.on_conflict_do_update(
                    index_elements=["key"],
                    set_=dict(
                        val=val,
                        description=description,
                        is_encrypted=is_encrypted,
                        team_name=team_name,
                    ),
                )
            elif dialect_name == "mysql":
                from sqlalchemy.dialects.mysql import insert as mysql_insert

                mysql_stmt = mysql_insert(Variable).values(
                    key=key,
                    val=val,
                    description=description,
                    is_encrypted=is_encrypted,
                    team_name=team_name,
                )
                stmt = mysql_stmt.on_duplicate_key_update(
                    val=val,
                    description=description,
                    is_encrypted=is_encrypted,
                    team_name=team_name,
                )
            else:
                from sqlalchemy.dialects.sqlite import insert as sqlite_insert

                sqlite_stmt = sqlite_insert(Variable).values(
                    key=key,
                    val=val,
                    description=description,
                    is_encrypted=is_encrypted,
                    team_name=team_name,
                )
                stmt = sqlite_stmt.on_conflict_do_update(
                    index_elements=["key"],
                    set_=dict(
                        val=val,
                        description=description,
                        is_encrypted=is_encrypted,
                        team_name=team_name,
                    ),
                )

            session.execute(stmt)
            # invalidate key in cache for faster propagation
            # we cannot save the value set because it's possible that it's shadowed by a custom backend
            # (see call to check_for_write_conflict above)
            SecretCache.invalidate_variable(key)

    @staticmethod
    def update(
        key: str,
        value: Any,
        serialize_json: bool = False,
        team_name: str | None = None,
        session: Session | None = None,
    ) -> None:
        """
        Update a given Airflow Variable with the Provided value.

        :param key: Variable Key
        :param value: Value to set for the Variable
        :param serialize_json: Serialize the value to a JSON string
        :param team_name: Team name associated to the variable (if any)
        :param session: optional session, use if provided or create a new one
        """
        # TODO: This is not the best way of having compat, but it's "better than erroring" for now. This still
        # means SQLA etc is loaded, but we can't avoid that unless/until we add import shims as a big
        # back-compat layer

        # If this is set it means are in some kind of execution context (Task, Dag Parse or Triggerer perhaps)
        # and should use the Task SDK API server path
        if hasattr(sys.modules.get("airflow.sdk.execution_time.task_runner"), "SUPERVISOR_COMMS"):
            warnings.warn(
                "Using Variable.update from `airflow.models` is deprecated."
                "Please use `set` on Variable from sdk(`airflow.sdk.Variable`) instead as it is an upsert.",
                DeprecationWarning,
                stacklevel=1,
            )
            from airflow.sdk import Variable as TaskSDKVariable

            # set is an upsert command, it can handle updates too
            TaskSDKVariable.set(
                key=key,
                value=value,
                serialize_json=serialize_json,
            )
            return

        if team_name and not conf.getboolean("core", "multi_team"):
            raise ValueError(
                "Multi-team mode is not configured in the Airflow environment. To assign a team to a variable, multi-mode must be enabled."
            )

        Variable.check_for_write_conflict(key=key)

        if Variable.get_variable_from_secrets(key=key, team_name=team_name) is None:
            raise KeyError(f"Variable {key} does not exist")

        ctx: contextlib.AbstractContextManager
        if session is not None:
            ctx = contextlib.nullcontext(session)
        else:
            ctx = create_session()

        with ctx as session:
            obj = session.scalar(
                select(Variable).where(
                    Variable.key == key, or_(Variable.team_name == team_name, Variable.team_name.is_(None))
                )
            )
            if obj is None:
                raise AttributeError(f"Variable {key} does not exist in the Database and cannot be updated.")

            Variable.set(
                key=key,
                value=value,
                description=obj.description,
                serialize_json=serialize_json,
                session=session,
            )

    @staticmethod
    def delete(key: str, team_name: str | None = None, session: Session | None = None) -> int:
        """
        Delete an Airflow Variable for a given key.

        :param key: Variable Keys
        :param team_name: Team name associated to the task trying to delete the variable (if any)
        :param session: optional session, use if provided or create a new one
        """
        # TODO: This is not the best way of having compat, but it's "better than erroring" for now. This still
        # means SQLA etc is loaded, but we can't avoid that unless/until we add import shims as a big
        # back-compat layer

        # If this is set it means are in some kind of execution context (Task, Dag Parse or Triggerer perhaps)
        # and should use the Task SDK API server path
        if hasattr(sys.modules.get("airflow.sdk.execution_time.task_runner"), "SUPERVISOR_COMMS"):
            warnings.warn(
                "Using Variable.delete from `airflow.models` is deprecated."
                "Please use `delete` on Variable from sdk(`airflow.sdk.Variable`) instead",
                DeprecationWarning,
                stacklevel=1,
            )
            from airflow.sdk import Variable as TaskSDKVariable

            TaskSDKVariable.delete(
                key=key,
            )
            return 1

        if team_name and not conf.getboolean("core", "multi_team"):
            raise ValueError(
                "Multi-team mode is not configured in the Airflow environment but the task trying to delete the variable belongs to a team"
            )

        from airflow.sdk import SecretCache

        ctx: contextlib.AbstractContextManager
        if session is not None:
            ctx = contextlib.nullcontext(session)
        else:
            ctx = create_session()

        with ctx as session:
            result = session.execute(
                delete(Variable).where(
                    Variable.key == key, or_(Variable.team_name == team_name, Variable.team_name.is_(None))
                )
            )
            rows = getattr(result, "rowcount", 0) or 0
            SecretCache.invalidate_variable(key)
            return rows

    def rotate_fernet_key(self):
        """Rotate Fernet Key."""
        fernet = get_fernet()
        if self._val and self.is_encrypted:
            self._val = fernet.rotate(self._val.encode("utf-8")).decode()

    @staticmethod
    def check_for_write_conflict(key: str) -> None:
        """
        Log a warning if a variable exists outside the metastore.

        If we try to write a variable to the metastore while the same key
        exists in an environment variable or custom secrets backend, then
        subsequent reads will not read the set value.

        :param key: Variable Key
        """
        for secrets_backend in ensure_secrets_loaded():
            if not isinstance(secrets_backend, MetastoreBackend):
                try:
                    var_val = secrets_backend.get_variable(key=key)
                    if var_val is not None:
                        _backend_name = type(secrets_backend).__name__
                        log.warning(
                            "The variable %s is defined in the %s secrets backend, which takes "
                            "precedence over reading from the database. The value in the database will be "
                            "updated, but to read it you have to delete the conflicting variable "
                            "from %s",
                            key,
                            _backend_name,
                            _backend_name,
                        )
                        return
                except Exception:
                    log.exception(
                        "Unable to retrieve variable from secrets backend (%s). "
                        "Checking subsequent secrets backend.",
                        type(secrets_backend).__name__,
                    )
            return None

    @staticmethod
    def get_variable_from_secrets(key: str, team_name: str | None = None) -> str | None:
        """
        Get Airflow Variable by iterating over all Secret Backends.

        :param key: Variable Key
        :param team_name: Team name associated to the task trying to access the variable (if any)
        :return: Variable Value
        """
        from airflow.sdk import SecretCache

        # Disable cache if the variable belongs to a team. We might enable it later
        if not team_name:
            # check cache first
            # enabled only if SecretCache.init() has been called first
            try:
                return SecretCache.get_variable(key)
            except SecretCache.NotPresentException:
                pass  # continue business

        var_val = None
        # iterate over backends if not in cache (or expired)
        for secrets_backend in ensure_secrets_loaded():
            try:
                var_val = secrets_backend.get_variable(key=key, team_name=team_name)
                if var_val is not None:
                    break
            except Exception:
                log.exception(
                    "Unable to retrieve variable from secrets backend (%s). "
                    "Checking subsequent secrets backend.",
                    type(secrets_backend).__name__,
                )

        SecretCache.save_variable(key, var_val)  # we save None as well
        return var_val

    @staticmethod
    @provide_session
    def get_team_name(variable_key: str, session=NEW_SESSION) -> str | None:
        stmt = select(Variable.team_name).where(Variable.key == variable_key)
        return session.scalar(stmt)

    @staticmethod
    @provide_session
    def get_key_to_team_name_mapping(variable_keys: list[str], session=NEW_SESSION) -> dict[str, str | None]:
        stmt = select(Variable.key, Variable.team_name).where(Variable.key.in_(variable_keys))
        return {key: team_name for key, team_name in session.execute(stmt)}
