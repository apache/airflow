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

from sqlalchemy import Boolean, Text
from sqlalchemy.orm import Mapped, declared_attr, mapped_column, synonym

# Preserve imports from the original module, including the shared Fernet cache.
from airflow.crypto import (
    FernetProtocol as FernetProtocol,
    _NullFernet as _NullFernet,
    _RealFernet as _RealFernet,
    get_fernet as get_fernet,
)


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
