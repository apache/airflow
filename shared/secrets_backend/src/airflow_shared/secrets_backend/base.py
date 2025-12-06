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

from abc import ABC


class BaseSecretsBackend(ABC):
    """Abstract base class to retrieve Connection object given a conn_id or Variable given a key."""

    _connection_class = None

    @classmethod
    def set_connection_class(cls, connection_class: type) -> None:
        """
        Set the Connection class to use for deserialization.

        :param connection_class: The Connection class to use.
        """
        cls._connection_class = connection_class

    @staticmethod
    def build_path(path_prefix: str, secret_id: str, sep: str = "/") -> str:
        """
        Given conn_id, build path for Secrets Backend.

        :param path_prefix: Prefix of the path to get secret
        :param secret_id: Secret id
        :param sep: separator used to concatenate connections_prefix and conn_id. Default: "/"
        """
        return f"{path_prefix}{sep}{secret_id}"

    def get_conn_value(self, conn_id: str) -> str | None:
        """
        Retrieve from Secrets Backend a string value representing the Connection object.

        If the client your secrets backend uses already returns a python dict, you should override
        ``get_connection`` instead.

        :param conn_id: connection id
        """
        raise NotImplementedError

    def get_variable(self, key: str) -> str | None:
        """
        Return value for Airflow Variable.

        :param key: Variable Key
        :return: Variable Value
        """
        raise NotImplementedError()

    def get_config(self, key: str) -> str | None:
        """
        Return value for Airflow Config Key.

        :param key: Config Key
        :return: Config Value
        """
        return None

    def deserialize_connection(
        self,
        conn_id: str,
        value: str,
    ):
        """
        Given a serialized representation of the airflow Connection, return an instance.

        Auto-detects which Connection class to use based on execution context.
        Uses Connection.from_json() for JSON format, Connection(uri=...) for URI format.

        :param conn_id: connection id
        :param value: the serialized representation of the Connection object
        :return: the deserialized Connection
        """
        if not self._connection_class:
            raise ValueError(
                "Connection class is not set. You must call `set_connection_class` on the class "
                "before calling deserialize_connection."
            )
        value = value.strip()
        if value[0] == "{":
            if hasattr(self._connection_class, "from_json"):
                return self._connection_class.from_json(value=value, conn_id=conn_id)
            raise ValueError(
                "Connection class does not support from_json deserialization: {self._connection_class}"
            )

        # TODO: Only sdk has from_uri defined on it. Is it worthwhile developing the core path or not?
        if hasattr(self._connection_class, "from_uri"):
            return self._connection_class.from_uri(conn_id=conn_id, uri=value)
        return self._connection_class(conn_id=conn_id, uri=value)

    def get_connection(self, conn_id: str):
        """
        Return connection object with a given ``conn_id``.

        :param conn_id: connection id
        :return: Connection object or None
        """
        value = self.get_conn_value(conn_id=conn_id)
        if value:
            return self.deserialize_connection(conn_id=conn_id, value=value)
        return None
