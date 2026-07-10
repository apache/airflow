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

import logging
from functools import cache
from typing import Protocol

from sqlalchemy import Boolean, Text
from sqlalchemy.orm import Mapped, declared_attr, mapped_column, synonym

from airflow.configuration import conf
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)


class FernetProtocol(Protocol):
    """This class is only used for TypeChecking (for IDEs, mypy, etc)."""

    is_encrypted: bool

    def decrypt(self, msg: bytes | str, ttl: int | None = None) -> bytes:
        """Decrypt with Fernet."""
        ...

    def encrypt(self, msg: bytes) -> bytes:
        """Encrypt with Fernet."""
        ...


class _NullFernet:
    """
    A "Null" encryptor class that doesn't encrypt or decrypt but that presents a similar interface to Fernet.

    The purpose of this is to make the rest of the code not have to know the
    difference, and to only display the message once, not 20 times when
    `airflow db migrate` is run.
    """

    is_encrypted = False

    def decrypt(self, msg: bytes | str, ttl: int | None = None) -> bytes:
        """Decrypt with Fernet."""
        if isinstance(msg, bytes):
            return msg
        if isinstance(msg, str):
            return msg.encode("utf-8")
        raise ValueError(f"Expected bytes or str, got {type(msg)}")

    def encrypt(self, msg: bytes) -> bytes:
        """Encrypt with Fernet."""
        return msg


class _RealFernet:
    """
    A wrapper around the real Fernet to set is_encrypted to True.

    This class is only used internally to avoid changing the interface of
    the get_fernet function.
    """

    from cryptography.fernet import Fernet, MultiFernet

    is_encrypted = True

    def __init__(self, fernet: MultiFernet):
        self._fernet = fernet

    def decrypt(self, msg: bytes | str, ttl: int | None = None) -> bytes:
        """Decrypt with Fernet."""
        return self._fernet.decrypt(msg, ttl)

    def encrypt(self, msg: bytes) -> bytes:
        """Encrypt with Fernet."""
        return self._fernet.encrypt(msg)

    def rotate(self, msg: bytes | str) -> bytes:
        """Rotate the Fernet key for the given message."""
        return self._fernet.rotate(msg)


class FernetFieldsMixin:
    """Mixin providing Fernet-encrypted ``password`` and ``extra`` fields."""

    _password: Mapped[str | None] = mapped_column("password", Text(), nullable=True)
    _extra: Mapped[str | None] = mapped_column("extra", Text(), nullable=True)
    is_encrypted: Mapped[bool] = mapped_column(Boolean, unique=False, default=False, nullable=False)
    is_extra_encrypted: Mapped[bool] = mapped_column(Boolean, unique=False, default=False, nullable=False)

    def get_password(self) -> str | None:
        """Decrypt and return password."""
        if self._password and self.is_encrypted:
            fernet = get_fernet()
            if not fernet.is_encrypted:
                raise ValueError("Can't decrypt encrypted password, FERNET_KEY configuration is missing")
            return fernet.decrypt(bytes(self._password, "utf-8")).decode()
        return self._password

    def set_password(self, value: str | None):
        """Encrypt and store password."""
        if value:
            fernet = get_fernet()
            self._password = fernet.encrypt(bytes(value, "utf-8")).decode()
            self.is_encrypted = fernet.is_encrypted

    @declared_attr
    def password(cls):
        """Password. The value is decrypted/encrypted when reading/setting the value."""
        return synonym("_password", descriptor=property(cls.get_password, cls.set_password))

    def get_extra(self) -> str | None:
        """Decrypt and return extra data."""
        if self._extra and self.is_extra_encrypted:
            fernet = get_fernet()
            if not fernet.is_encrypted:
                raise ValueError("Can't decrypt `extra` params, FERNET_KEY configuration is missing")
            return fernet.decrypt(bytes(self._extra, "utf-8")).decode()
        return self._extra

    def set_extra(self, value: str | None):
        """Encrypt and store extra data."""
        if value:
            fernet = get_fernet()
            self._extra = fernet.encrypt(bytes(value, "utf-8")).decode()
            self.is_extra_encrypted = fernet.is_encrypted
        else:
            self._extra = value
            self.is_extra_encrypted = False

    @declared_attr
    def extra(cls):
        """Extra data. The value is decrypted/encrypted when reading/setting the value."""
        return synonym("_extra", descriptor=property(cls.get_extra, cls.set_extra))


@cache
def get_fernet() -> FernetProtocol:
    """
    Deferred load of Fernet key.

    This function could fail either because Cryptography is not installed
    or because the Fernet key is invalid.

    :return: Fernet object
    :raises: airflow.exceptions.AirflowException if there's a problem trying to load Fernet
    """
    from cryptography.fernet import Fernet, MultiFernet

    try:
        fernet_key = conf.get("core", "FERNET_KEY")
        if not fernet_key:
            log.warning("empty cryptography key - values will not be stored encrypted.")
            return _NullFernet()

        fernet = MultiFernet([Fernet(fernet_part.encode("utf-8")) for fernet_part in fernet_key.split(",")])
        return _RealFernet(fernet)
    except (ValueError, TypeError) as value_error:
        raise AirflowException(f"Could not create Fernet object: {value_error}")
