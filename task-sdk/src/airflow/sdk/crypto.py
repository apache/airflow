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
from typing import TYPE_CHECKING, Protocol

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from cryptography.fernet import MultiFernet


class FernetProtocol(Protocol):
    """
    Protocol for Fernet encryption/decryption.

    This class is only used for TypeChecking (for IDEs, mypy, etc).

    Note: The rotate() method exists on _RealFernet but is not part of this Protocol.
    rotate() should only be called from CLI commands where encryption is guaranteed to be
    enabled. _NullFernet (used in unit tests without FERNET_KEY) does not support rotation.
    """

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


@cache
def get_fernet() -> FernetProtocol:
    """
    Deferred load of Fernet key from SDK configuration.

    This function could fail either because Cryptography is not installed
    or because the Fernet key is invalid.

    :return: Fernet object
    :raises: airflow.sdk.exceptions.AirflowException if there's a problem trying to load Fernet
    """
    from cryptography.fernet import Fernet, MultiFernet

    from airflow.sdk.configuration import conf
    from airflow.sdk.exceptions import AirflowException

    try:
        fernet_key = conf.get("core", "FERNET_KEY")
        if not fernet_key:
            log.warning("empty cryptography key - values will not be stored encrypted.")
            return _NullFernet()

        fernet = MultiFernet([Fernet(fernet_part.encode("utf-8")) for fernet_part in fernet_key.split(",")])
        return _RealFernet(fernet)
    except (ValueError, TypeError) as value_error:
        raise AirflowException(f"Could not create Fernet object: {value_error}")
