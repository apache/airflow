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

import json
import logging
from typing import Any

from sqlalchemy import Boolean, Column, Integer, String, Text, delete
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.orm import Session, declared_attr, reconstructor, synonym

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.configuration import ensure_secrets_loaded
from airflow.models.base import ID_LEN, Base
from airflow.models.crypto import get_fernet
from airflow.secrets.cache import SecretCache
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.secrets_masker import mask_secret
from airflow.utils.session import provide_session

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

    def __init__(self, key=None, val=None, description=None):
        super().__init__()
        self.key = key
        self.val = val
        self.description = description

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
        :return: Mixed
        """
        obj = Variable.get(key, default_var=None, deserialize_json=deserialize_json)
        if obj is None:
            if default is not None:
                Variable.set(key, default, description=description, serialize_json=deserialize_json)
                return default
            else:
                raise ValueError("Default Value must be set")
        else:
            return obj

    @classmethod
    def get(
        cls,
        key: str,
        default_var: Any = __NO_DEFAULT_SENTINEL,
        deserialize_json: bool = False,
    ) -> Any:
        """Gets a value for an Airflow Variable Key.

        :param key: Variable Key
        :param default_var: Default value of the Variable if the Variable doesn't exist
        :param deserialize_json: Deserialize the value to a Python dict
        """
        var_val = Variable.get_variable_from_secrets(key=key)
        if var_val is None:
            if default_var is not cls.__NO_DEFAULT_SENTINEL:
                return default_var
            else:
                raise KeyError(f"Variable {key} does not exist")
        else:
            if deserialize_json:
                obj = json.loads(var_val)
                mask_secret(obj, key)
                return obj
            else:
                mask_secret(var_val, key)
                return var_val

    @staticmethod
    @provide_session
    @internal_api_call
    def set(
        key: str,
        value: Any,
        description: str | None = None,
        serialize_json: bool = False,
        session: Session = None,
    ) -> None:
        """Sets a value for an Airflow Variable with a given Key.

        This operation overwrites an existing variable.

        :param key: Variable Key
        :param value: Value to set for the Variable
        :param description: Description of the Variable
        :param serialize_json: Serialize the value to a JSON string
        """
        # check if the secret exists in the custom secrets' backend.
        Variable.check_for_write_conflict(key)
        if serialize_json:
            stored_value = json.dumps(value, indent=2)
        else:
            stored_value = str(value)

        Variable.delete(key, session=session)
        session.add(Variable(key=key, val=stored_value, description=description))
        session.flush()
        # invalidate key in cache for faster propagation
        # we cannot save the value set because it's possible that it's shadowed by a custom backend
        # (see call to check_for_write_conflict above)
        SecretCache.invalidate_variable(key)

    @staticmethod
    @provide_session
    @internal_api_call
    def update(
        key: str,
        value: Any,
        serialize_json: bool = False,
        session: Session = None,
    ) -> None:
        """Updates a given Airflow Variable with the Provided value.

        :param key: Variable Key
        :param value: Value to set for the Variable
        :param serialize_json: Serialize the value to a JSON string
        """
        Variable.check_for_write_conflict(key)

        if Variable.get_variable_from_secrets(key=key) is None:
            raise KeyError(f"Variable {key} does not exist")

        obj = session.query(Variable).filter(Variable.key == key).first()
        if obj is None:
            raise AttributeError(f"Variable {key} does not exist in the Database and cannot be updated.")

        Variable.set(key, value, description=obj.description, serialize_json=serialize_json)

    @staticmethod
    @provide_session
    @internal_api_call
    def delete(key: str, session: Session = None) -> int:
        """Delete an Airflow Variable for a given key.

        :param key: Variable Keys
        """
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
        """Logs a warning if a variable exists outside of the metastore.

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
                        log.warning(
                            "The variable {key} is defined in the {cls} secrets backend, which takes "
                            "precedence over reading from the database. The value in the database will be "
                            "updated, but to read it you have to delete the conflicting variable "
                            "from {cls}".format(key=key, cls=secrets_backend.__class__.__name__)
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
