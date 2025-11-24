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

from typing import TYPE_CHECKING

from airflow.sdk._shared.secrets_backend.base import BaseSecretsBackend as BaseSecretsBackendFromShared

if TYPE_CHECKING:
    from airflow.sdk.definitions.connection import Connection


class BaseSecretsBackend(BaseSecretsBackendFromShared):
    """Extends the shared base class by adding methods that work with Connection objects using the SDK Connection class."""

    def deserialize_connection(self, conn_id: str, value: str) -> Connection:
        """
        Given a serialized representation of the Connection, return an instance.

        Looks at first character to determine how to deserialize.

        :param conn_id: connection id
        :param value: the serialized representation of the Connection object
        :return: the deserialized Connection
        """
        from airflow.sdk.definitions.connection import Connection

        value = value.strip()
        if value[0] == "{":
            return Connection.from_json(conn_id=conn_id, value=value)
        return Connection.from_uri(conn_id=conn_id, uri=value)

    def get_connection(self, conn_id: str) -> Connection | None:
        """
        Return connection object with a given ``conn_id``.

        :param conn_id: connection id
        """
        value = self.get_conn_value(conn_id=conn_id)

        if value:
            return self.deserialize_connection(conn_id=conn_id, value=value)
        return None
