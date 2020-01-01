# -*- coding: utf-8 -*-
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
from typing import Any

from cryptography.fernet import InvalidToken as InvalidFernetToken
from sqlalchemy import Boolean, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import synonym

from airflow.models.base import ID_LEN, Base
from airflow.models.crypto import get_fernet
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session


class Variable(Base, LoggingMixin):
    __tablename__ = "variable"
    __NO_DEFAULT_SENTINEL = object()

    id = Column(Integer, primary_key=True)
    key = Column(String(ID_LEN), unique=True)
    _val = Column('val', Text)
    is_encrypted = Column(Boolean, unique=False, default=False)

    def __repr__(self):
        # Hiding the value
        return '{} : {}'.format(self.key, self._val)

    def get_val(self):
        log = LoggingMixin().log
        if self._val and self.is_encrypted:
            try:
                fernet = get_fernet()
                return fernet.decrypt(bytes(self._val, 'utf-8')).decode()
            except InvalidFernetToken:
                log.error("Can't decrypt _val for key=%s, invalid token or value", self.key)
                return None
            except Exception:
                log.error("Can't decrypt _val for key=%s, FERNET_KEY configuration missing", self.key)
                return None
        else:
            return self._val

    def set_val(self, value):
        if value:
            fernet = get_fernet()
            self._val = fernet.encrypt(bytes(value, 'utf-8')).decode()
            self.is_encrypted = fernet.is_encrypted

    @declared_attr
    def val(cls):
        return synonym('_val',
                       descriptor=property(cls.get_val, cls.set_val))

    @classmethod
    def setdefault(cls, key, default, deserialize_json=False):
        """
        Like a Python builtin dict object, setdefault returns the current value
        for a key, and if it isn't there, stores the default value and returns it.

        :param key: Dict key for this Variable
        :type key: str
        :param default: Default value to set and return if the variable
            isn't already in the DB
        :type default: Mixed
        :param deserialize_json: Store this as a JSON encoded value in the DB
            and un-encode it when retrieving a value
        :return: Mixed
        """
        obj = Variable.get(key, default_var=None,
                           deserialize_json=deserialize_json)
        if obj is None:
            if default is not None:
                Variable.set(key, default, serialize_json=deserialize_json)
                return default
            else:
                raise ValueError('Default Value must be set')
        else:
            return obj

    @classmethod
    @provide_session
    def get(
        cls,
        key: str,
        default_var: Any = __NO_DEFAULT_SENTINEL,
        deserialize_json: bool = False,
        session=None
    ):
        obj = session.query(cls).filter(cls.key == key).first()
        if obj is None:
            if default_var is not cls.__NO_DEFAULT_SENTINEL:
                return default_var
            else:
                raise KeyError('Variable {} does not exist'.format(key))
        else:
            if deserialize_json:
                return json.loads(obj.val)
            else:
                return obj.val

    @classmethod
    @provide_session
    def set(
        cls,
        key: str,
        value: Any,
        serialize_json: bool = False,
        session=None
    ):

        if serialize_json:
            stored_value = json.dumps(value, indent=2)
        else:
            stored_value = str(value)

        Variable.delete(key, session=session)
        session.add(Variable(key=key, val=stored_value))  # type: ignore
        session.flush()

    @classmethod
    @provide_session
    def delete(cls, key, session=None):
        session.query(cls).filter(cls.key == key).delete()

    def rotate_fernet_key(self):
        fernet = get_fernet()
        if self._val and self.is_encrypted:
            self._val = fernet.rotate(self._val.encode('utf-8')).decode()
