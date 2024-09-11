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

import json
import logging
from typing import Any, Optional

from cryptography.fernet import InvalidToken as InvalidFernetToken
from sqlalchemy import Boolean, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import Session, reconstructor, synonym

from airflow.configuration import ensure_secrets_loaded
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models.base import ID_LEN, Base
from airflow.models.crossdagcommunication import CrossDagComm
from airflow.models.crypto import get_fernet
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.secrets_masker import mask_secret
from airflow.utils.session import provide_session
from airflow.operators.python import get_current_context

log = logging.getLogger(__name__)


class Variable(Base, LoggingMixin):
    """
    Variables are a generic way to store and retrieve arbitrary content or settings
    as a simple key value store within Airflow.
    """

    __tablename__ = "variable"
    __NO_DEFAULT_SENTINEL = object()

    id = Column(Integer, primary_key=True)
    key = Column(String(ID_LEN), unique=True)
    _val = Column('val', Text)
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
        return f'{self.key} : {self._val}'

    def get_val(self):
        """Get Airflow Variable from Metadata DB and decode it using the Fernet Key"""
        if self._val is not None and self.is_encrypted:
            try:
                fernet = get_fernet()
                return fernet.decrypt(bytes(self._val, 'utf-8')).decode()
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
            self._val = fernet.encrypt(bytes(value, 'utf-8')).decode()
            self.is_encrypted = fernet.is_encrypted

    @declared_attr
    def val(cls):
        """Get Airflow Variable from Metadata DB and decode it using the Fernet Key"""
        return synonym('_val', descriptor=property(cls.get_val, cls.set_val))

    @classmethod
    def setdefault(cls, key, default, description=None, deserialize_json=False):
        """
        Like a Python builtin dict object, setdefault returns the current value
        for a key, and if it isn't there, stores the default value and returns it.

        :param key: Dict key for this Variable
        :param default: Default value to set and return if the variable
            isn't already in the DB
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
                raise ValueError('Default Value must be set')
        else:
            return obj

    @classmethod
    def get(
        cls,
        key: str,
        default_var: Any = __NO_DEFAULT_SENTINEL,
        deserialize_json: bool = False,
    ) -> Any:
        """
        Gets a value for an Airflow Variable Key

        :param key: Variable Key
        :param default_var: Default value of the Variable if the Variable doesn't exist
        :param deserialize_json: Deserialize the value to a Python dict
        """
        try:
            context = get_current_context()
            ti = context["ti"]
            CrossDagComm.add(
                type="variable",
                key=key,
                target_dag_id=ti.dag_id,
                target_task_id=ti.task_id,
            )
        except AirflowException:
            log.warning("Variable.get() called outside of task context.")

        var_val = Variable.get_variable_from_secrets(key=key)
        if var_val is None:
            if default_var is not cls.__NO_DEFAULT_SENTINEL:
                return default_var
            else:
                raise KeyError(f'Variable {key} does not exist')
        else:
            if deserialize_json:
                obj = json.loads(var_val)
                mask_secret(var_val, key)
                return obj
            else:
                mask_secret(var_val, key)
                return var_val

    @classmethod
    @provide_session
    def set(
        cls,
        key: str,
        value: Any,
        description: Optional[str] = None,
        serialize_json: bool = False,
        session: Session = None,
        is_multi_cluster_enabled: bool = False,
    ):
        """
        Sets a value for an Airflow Variable with a given Key.
        This operation will overwrite an existing variable.

        :param key: Variable Key
        :param value: Value to set for the Variable
        :param description: Description of the Variable
        :param serialize_json: Serialize the value to a JSON string
        :param session: SQL Alchemy Sessions
        :param is_multi_cluster_enabled: is set wrapped by lyft-etl sync_variable_writes_multicluster
        """
        try:
            context = get_current_context()
            ti = context["ti"]
            CrossDagComm.add(
                type="variable",
                key=key,
                source_dag_id=ti.dag_id,
                source_task_id=ti.task_id,
            )
        except AirflowException:
            log.warning("Variable.set() called outside of task context.")
        
        # check if the secret exists in the custom secrets backend.
        cls.check_for_write_conflict(key)
        if serialize_json:
            stored_value = json.dumps(value, indent=2)
        else:
            stored_value = str(value)

        Variable.delete(key, session=session)
        session.add(Variable(key=key, val=stored_value, description=description))
        session.flush()

    @classmethod
    @provide_session
    def update(
        cls,
        key: str,
        value: Any,
        serialize_json: bool = False,
        session: Session = None,
    ):
        """
        Updates a given Airflow Variable with the Provided value

        :param key: Variable Key
        :param value: Value to set for the Variable
        :param serialize_json: Serialize the value to a JSON string
        :param session: SQL Alchemy Session
        """
        cls.check_for_write_conflict(key)

        if cls.get_variable_from_secrets(key=key) is None:
            raise KeyError(f'Variable {key} does not exist')

        obj = session.query(cls).filter(cls.key == key).first()
        if obj is None:
            raise AttributeError(f'Variable {key} does not exist in the Database and cannot be updated.')

        cls.set(key, value, description=obj.description, serialize_json=serialize_json)

    @classmethod
    @provide_session
    def delete(cls, key: str, session: Session = None) -> int:
        """
        Delete an Airflow Variable for a given key

        :param key: Variable Key
        :param session: SQL Alchemy Sessions
        """
        return session.query(cls).filter(cls.key == key).delete()

    def rotate_fernet_key(self):
        """Rotate Fernet Key"""
        fernet = get_fernet()
        if self._val and self.is_encrypted:
            self._val = fernet.rotate(self._val.encode('utf-8')).decode()

    @staticmethod
    def check_for_write_conflict(key: str) -> None:
        """
        Logs a warning if a variable exists outside of the metastore.

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
                        'Unable to retrieve variable from secrets backend (%s). '
                        'Checking subsequent secrets backend.',
                        type(secrets_backend).__name__,
                    )
            return None

    @staticmethod
    def get_variable_from_secrets(key: str) -> Optional[str]:
        """
        Get Airflow Variable by iterating over all Secret Backends.

        :param key: Variable Key
        :return: Variable Value
        """
        for secrets_backend in ensure_secrets_loaded():
            try:
                var_val = secrets_backend.get_variable(key=key)
                if var_val is not None:
                    return var_val
            except Exception:
                log.exception(
                    'Unable to retrieve variable from secrets backend (%s). '
                    'Checking subsequent secrets backend.',
                    type(secrets_backend).__name__,
                )
        return None
