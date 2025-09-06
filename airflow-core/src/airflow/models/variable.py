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

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Text, delete, select
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.orm import declared_attr, reconstructor, synonym
from sqlalchemy_utils import UUIDType

from airflow._shared.secrets_masker import mask_secret
from airflow.configuration import ensure_secrets_loaded
from airflow.models.base import ID_LEN, Base
from airflow.models.crypto import get_fernet
from airflow.models.team import Team
from airflow.sdk import SecretCache
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, create_session, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)


class Variable(Base, LoggingMixin):
    """A generic way to store and retrieve arbitrary content or settings as a simple key/value store."""

    __tablename__ = "variable"
    __NO_DEFAULT_SENTINEL = object()

    id = Column(Integer, primary_key=True)
    key = Column(String(ID_LEN), unique=True)
    _val = Column("val", Text().with_variant(MEDIUMTEXT, "mysql"))
    description = Column(Text)
    is_encrypted = Column(Boolean, unique=False, default=False)
    team_id = Column(UUIDType(binary=False), ForeignKey("team.id"), nullable=True)

    def __init__(self, key=None, val=None, description=None, team_id=None):
        super().__init__()
        self.key = key
        self.val = val
        self.description = description
        self.team_id = team_id

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
    ) -> Any:
        """
        Get a value for an Airflow Variable Key.

        :param key: Variable Key
        :param default_var: Default value of the Variable if the Variable doesn't exist
        :param deserialize_json: Deserialize the value to a Python dict
        """
        # TODO: This is not the best way of having compat, but it's "better than erroring" for now. This still
        # means SQLA etc is loaded, but we can't avoid that unless/until we add import shims as a big
        # back-compat layer

        # If this is set it means are in some kind of execution context (Task, Dag Parse or Triggerer perhaps)
        # and should use the Task SDK API server path
        if hasattr(sys.modules.get("airflow.sdk.execution_time.task_runner"), "SUPERVISOR_COMMS"):
            warnings.warn(
                "Using Variable.get from `airflow.models` is deprecated."
                "Please use `get` on Variable from sdk(`airflow.sdk.Variable`) instead",
                DeprecationWarning,
                stacklevel=1,
            )
            from airflow.sdk import Variable as TaskSDKVariable
            from airflow.sdk.definitions._internal.types import NOTSET

            var_val = TaskSDKVariable.get(
                key,
                default=NOTSET if default_var is cls.__NO_DEFAULT_SENTINEL else default_var,
                deserialize_json=deserialize_json,
            )
            if isinstance(var_val, str):
                mask_secret(var_val, key)

            return var_val

        var_val = Variable.get_variable_from_secrets(key=key)
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
        session: Session | None = None,
    ) -> None:
        """
        Set a value for an Airflow Variable with a given Key.

        This operation overwrites an existing variable using the session's dialect-specific upsert operation.

        :param key: Variable Key
        :param value: Value to set for the Variable
        :param description: Description of the Variable
        :param serialize_json: Serialize the value to a JSON string
        :param session: optional session, use if provided or create a new one
        """
        # TODO: This is not the best way of having compat, but it's "better than erroring" for now. This still
        # means SQLA etc is loaded, but we can't avoid that unless/until we add import shims as a big
        # back-compat layer

        # If this is set it means are in some kind of execution context (Task, Dag Parse or Triggerer perhaps)
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

        # check if the secret exists in the custom secrets' backend.
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
            new_variable = Variable(key=key, val=stored_value, description=description)

            val = new_variable._val
            is_encrypted = new_variable.is_encrypted

            # Import dialect-specific insert function
            if (dialect_name := session.get_bind().dialect.name) == "postgresql":
                from sqlalchemy.dialects.postgresql import insert
            elif dialect_name == "mysql":
                from sqlalchemy.dialects.mysql import insert
            else:
                from sqlalchemy.dialects.sqlite import insert

            # Create the insert statement (common for all dialects)
            stmt = insert(Variable).values(
                key=key,
                val=val,
                description=description,
                is_encrypted=is_encrypted,
            )

            # Apply dialect-specific upsert
            if dialect_name == "mysql":
                # MySQL: ON DUPLICATE KEY UPDATE
                stmt = stmt.on_duplicate_key_update(
                    val=val,
                    description=description,
                    is_encrypted=is_encrypted,
                )
            else:
                # PostgreSQL and SQLite: ON CONFLICT DO UPDATE
                stmt = stmt.on_conflict_do_update(
                    index_elements=["key"],
                    set_=dict(
                        val=val,
                        description=description,
                        is_encrypted=is_encrypted,
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
        session: Session | None = None,
    ) -> None:
        """
        Update a given Airflow Variable with the Provided value.

        :param key: Variable Key
        :param value: Value to set for the Variable
        :param serialize_json: Serialize the value to a JSON string
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

        Variable.check_for_write_conflict(key=key)

        if Variable.get_variable_from_secrets(key=key) is None:
            raise KeyError(f"Variable {key} does not exist")

        ctx: contextlib.AbstractContextManager
        if session is not None:
            ctx = contextlib.nullcontext(session)
        else:
            ctx = create_session()

        with ctx as session:
            obj = session.scalar(select(Variable).where(Variable.key == key))
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
    def delete(key: str, session: Session | None = None) -> int:
        """
        Delete an Airflow Variable for a given key.

        :param key: Variable Keys
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

        ctx: contextlib.AbstractContextManager
        if session is not None:
            ctx = contextlib.nullcontext(session)
        else:
            ctx = create_session()

        with ctx as session:
            rows = session.execute(delete(Variable).where(Variable.key == key)).rowcount
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
    def get_variable_from_secrets(key: str) -> str | None:
        """
        Get Airflow Variable by iterating over all Secret Backends.

        :param key: Variable Key
        :return: Variable Value
        """
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
                var_val = secrets_backend.get_variable(key=key)
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
        stmt = (
            select(Team.name).join(Variable, Team.id == Variable.team_id).where(Variable.key == variable_key)
        )
        return session.scalar(stmt)
