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

from abc import abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


class BaseUser:
    """User model interface. These attributes/methods should be implemented in the pluggable auth manager."""

    id: int | str | None
    first_name: str | None
    last_name: str | None
    username: str | None
    role: str | None
    email: str | None
    active: bool | None
    roles: list[Any] | None

    @abstractmethod
    def get_id(self) -> str:
        """Get user ID."""
        ...

    @abstractmethod
    def get_name(self) -> str:
        """Get user name."""
        ...

    @abstractmethod
    def get_groups(self) -> list[str] | None:
        """Get user groups."""
        ...
