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

import inspect
from abc import ABC
from collections.abc import Callable


def _accepts_team_name(method: Callable) -> bool:
    """
    Return whether a secrets-backend method accepts the ``team_name`` keyword.

    Backends written before Airflow 3.2 override ``get_conn_value`` / ``get_variable`` /
    ``get_connection`` with the legacy ``(self, conn_id)`` / ``(self, key)`` signature.
    AIP-67 (multi-team) added a ``team_name`` keyword; forwarding it to those raises
    ``TypeError``. A method accepts it if it declares a ``team_name`` parameter or a
    ``**kwargs`` catch-all.
    """
    try:
        parameters = inspect.signature(method).parameters
    except (TypeError, ValueError):
        # Un-introspectable callable (e.g. C-implemented): assume the 3.2+ signature.
        return True
    return "team_name" in parameters or any(
        p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values()
    )


def call_secrets_backend_method(method: Callable, *, team_name: str | None, **kwargs):
    """
    Call a secrets-backend lookup ``method``, forwarding ``team_name`` only when supported.

    Forward ``team_name`` to backends that accept it (3.2+ overrides) and omit it for
    pre-3.2 overrides, so older bundled providers and custom backends keep working --
    in both single-team and multi-team deployments -- without being forced to add the
    parameter. A ``TypeError`` raised inside an accepting backend is left to propagate
    rather than retried without ``team_name``, which could mask the error and resolve a
    team-scoped lookup against the global scope.
    """
    if _accepts_team_name(method):
        return method(team_name=team_name, **kwargs)
    return method(**kwargs)


class BaseSecretsBackend(ABC):
    """Abstract base class to retrieve Connection object given a conn_id or Variable given a key."""

    @staticmethod
    def build_path(path_prefix: str, secret_id: str, sep: str = "/") -> str:
        """
        Given conn_id, build path for Secrets Backend.

        :param path_prefix: Prefix of the path to get secret
        :param secret_id: Secret id
        :param sep: separator used to concatenate connections_prefix and conn_id. Default: "/"
        """
        return f"{path_prefix}{sep}{secret_id}"

    def get_conn_value(self, conn_id: str, team_name: str | None = None) -> str | None:
        """
        Retrieve from Secrets Backend a string value representing the Connection object.

        If the client your secrets backend uses already returns a python dict, you should override
        ``get_connection`` instead.

        :param conn_id: connection id
        :param team_name: Team name associated to the task trying to access the connection (if any)
        """
        raise NotImplementedError

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        """
        Return value for Airflow Variable.

        :param key: Variable Key
        :param team_name: Team name associated to the task trying to access the variable (if any)
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

    def _set_connection_class(self, conn_class: type) -> None:
        if not isinstance(conn_class, type):
            raise TypeError(f"Connection class must be a type/class, got {type(conn_class).__name__}")
        self._connection_class = conn_class

    def _get_connection_class(self) -> type:
        """Get the Connection class to use for deserialization."""
        conn_class = getattr(self, "_connection_class", None)
        if conn_class is None:
            raise RuntimeError(
                "Connection class not set on backend instance. "
                "Backends must be instantiated via initialize_secrets_backends() "
                "or have _connection_class set manually."
            )
        return conn_class

    @staticmethod
    def _deserialize_connection_value(conn_class: type, conn_id: str, value: str):
        value = value.strip()
        if value[0] == "{":
            return conn_class.from_json(value=value, conn_id=conn_id)  # type: ignore[attr-defined]

        # TODO: Only sdk has from_uri defined on it. Is it worthwhile developing the core path or not?
        if hasattr(conn_class, "from_uri"):
            return conn_class.from_uri(conn_id=conn_id, uri=value)
        return conn_class(conn_id=conn_id, uri=value)

    def deserialize_connection(self, conn_id: str, value: str):
        """
        Given a serialized representation of the airflow Connection, return an instance.

        Uses the Connection class set on this class (which should be set to the appropriate Connection class for the execution context).
        Uses Connection.from_json() for JSON format, Connection(uri=...) for URI format.

        :param conn_id: connection id
        :param value: the serialized representation of the Connection object
        :return: the deserialized Connection
        """
        conn_class = self._get_connection_class()
        return self._deserialize_connection_value(conn_class, conn_id, value)

    def get_connection(self, conn_id: str, team_name: str | None = None):
        """
        Return connection object with a given ``conn_id``.

        :param conn_id: connection id
        :param team_name: Team name associated to the task trying to access the connection (if any)
        :return: Connection object or None
        """
        value = call_secrets_backend_method(self.get_conn_value, team_name=team_name, conn_id=conn_id)
        if value:
            return self.deserialize_connection(conn_id=conn_id, value=value)
        return None
