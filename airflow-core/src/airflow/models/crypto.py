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

from airflow.configuration import conf
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)


class FernetProtocol(Protocol):
    """This class is only used for TypeChecking (for IDEs, mypy, etc)."""

    def decrypt(self, msg: bytes | str, ttl: int | None = None) -> bytes:
        """Decrypt with Fernet."""
        ...

    def encrypt(self, msg: bytes) -> bytes:
        """Encrypt with Fernet."""
        ...

    def rotate(self, msg: bytes | str) -> bytes:
        """Rotate the Fernet key for the given message."""
        ...


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
        fernet_key = conf.get_mandatory_value("core", "FERNET_KEY")
        return MultiFernet([Fernet(fernet_part.encode("utf-8")) for fernet_part in fernet_key.split(",")])
    except (ValueError, TypeError) as value_error:
        raise AirflowException(f"Could not create Fernet object: {value_error}")
