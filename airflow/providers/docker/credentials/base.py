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

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Sequence

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.secrets_masker import mask_secret

if TYPE_CHECKING:
    from airflow.models import Connection


@dataclass
class DockerLoginCredentials:
    """Class for keeping authentication information for Docker Registry."""

    username: str
    password: str
    registry: str
    email: Optional[str] = None
    reauth: bool = False

    def __post_init__(self):
        if self.password:
            # Mask password if it specified.
            mask_secret(self.password)


class BaseDockerCredentialHelper(LoggingMixin):
    """Base class for authentication in Docker Registry.

    :param conn: Reference to Docker hook connection object.
    """

    def __init__(self, *, conn: "Connection", **kwargs):
        super().__init__()
        self.conn = conn
        self.conn_extra = conn.extra_dejson

    @property
    def reauth(self) -> bool:
        """The reauth property from connection."""
        val = self.conn_extra.get("reauth", True)
        if isinstance(val, bool):
            return val
        val = str(val).lower()
        if val in ('y', 'yes', 't', 'true', 'on', '1'):
            return True
        if val in ('n', 'no', 'f', 'false', 'off', '0'):
            return False
        raise ValueError(f"{val!r} is not a boolean-like string value.")

    @property
    def email(self) -> Optional[str]:
        """The email for the registry account from connection."""
        return self.conn_extra.get("email")

    def get_credentials(self) -> Optional[Sequence[DockerLoginCredentials]]:
        """
        Method uses for return credentials to Docker Registry.
        It might or might not use credentials from Connection.
        """
        raise NotImplementedError()
