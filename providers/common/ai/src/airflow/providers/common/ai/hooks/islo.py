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
"""Hook for the islo.dev sandbox API."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from islo import Islo


class IsloHook(BaseHook):
    """
    Bridge an Airflow connection to an `islo.dev <https://islo.dev>`__ SDK client.

    :class:`~airflow.providers.common.ai.sandbox.IsloSandboxBackend` resolves its
    credentials through this hook; call it directly when a task needs the client
    for something the backend does not cover.

    Connection fields:

    * **password**: the Islo API key. Required.
    * **host**: optional compute URL, passed as ``compute_url=`` -- the regional
      API the microVMs run on. The SDK default is ``https://ca.compute.islo.dev``.
    * **extra** JSON: optional ``base_url`` (control-plane URL, SDK default
      ``https://api.islo.dev``) and ``timeout`` (request timeout in seconds).

    :param islo_conn_id: Airflow connection ID. Falls back to
        :attr:`default_conn_name` (``"islo_default"``) if not provided.
    """

    conn_name_attr = "islo_conn_id"
    default_conn_name = "islo_default"
    conn_type = "islo"
    hook_name = "Islo"

    def __init__(self, islo_conn_id: str | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.islo_conn_id = islo_conn_id if islo_conn_id is not None else self.default_conn_name

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "API Key", "host": "Compute URL"},
            "placeholders": {
                "host": "https://ca.compute.islo.dev (optional, the regional compute API)",
                "extra": '{"base_url": "https://api.islo.dev", "timeout": 30}',
            },
        }

    def build_client_kwargs(self) -> dict[str, Any]:
        """Translate the connection into ``islo.Islo`` constructor arguments."""
        conn = self.get_connection(self.islo_conn_id)
        api_key = (conn.password or "").strip()
        if not api_key:
            raise ValueError(f"Connection {self.islo_conn_id!r} has no password; set it to the Islo API key.")
        kwargs: dict[str, Any] = {"api_key": api_key}
        if conn.host:
            kwargs["compute_url"] = conn.host
        extra = conn.extra_dejson
        if extra.get("base_url"):
            kwargs["base_url"] = extra["base_url"]
        if extra.get("timeout") is not None:
            try:
                timeout = float(extra["timeout"])
            except (TypeError, ValueError) as e:
                raise ValueError("The Islo connection extra timeout must be a positive finite number.") from e
            if not math.isfinite(timeout) or timeout <= 0:
                raise ValueError("The Islo connection extra timeout must be a positive finite number.")
            kwargs["timeout"] = timeout
        return kwargs

    def get_conn(self) -> Islo:
        """Return an authenticated ``islo.Islo`` client."""
        from islo import Islo

        return Islo(**self.build_client_kwargs())

    def test_connection(self) -> tuple[bool, str]:
        """Check the credentials with a one-item listing; nothing is created."""
        try:
            self.get_conn().sandboxes.list_sandboxes(limit=1)
        except Exception as e:
            return False, str(e)
        return True, "Connection successfully tested"
