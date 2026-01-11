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

import subprocess
from abc import ABC
from typing import Any

from airflow.providers.common.compat.sdk import AirflowException, BaseHook


class TtuHook(BaseHook, ABC):
    """
    Abstract base hook for integrating Teradata Tools and Utilities (TTU) in Airflow.

    This hook provides common connection handling, resource management, and lifecycle
    support for TTU based operations such as BTEQ, TLOAD, and TPT.

    It should not be used directly. Instead, it must be subclassed by concrete hooks
    like `BteqHook`, `TloadHook`, or `TddlHook` that implement the actual TTU command logic.

    Core Features:
    - Establishes a reusable Teradata connection configuration.
    - Provides context management for safe resource cleanup.
    - Manages subprocess termination (e.g., for long-running TTU jobs).

    Requirements:
    - TTU command-line tools must be installed and accessible via PATH.
    - A valid Airflow connection with Teradata credentials must be configured.
    """

    def __init__(self, teradata_conn_id: str = "teradata_default", *args, **kwargs) -> None:
        super().__init__()
        self.teradata_conn_id = teradata_conn_id
        self.conn: dict[str, Any] | None = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.conn is not None:
            self.close_conn()

    def get_conn(self) -> dict[str, Any]:
        """
        Set up and return a Teradata connection dictionary.

        This dictionary includes connection credentials and a subprocess placeholder.
        Ensures connection is created only once per hook instance.

        :return: Dictionary with connection details.
        """
        if not self.conn:
            connection = self.get_connection(self.teradata_conn_id)
            if not connection.login or not connection.password or not connection.host:
                raise AirflowException("Missing required connection parameters: login, password, or host.")

            self.conn = dict(
                login=connection.login,
                password=connection.password,
                host=connection.host,
                database=connection.schema,
                sp=None,  # Subprocess placeholder
            )
        return self.conn

    def close_conn(self):
        """Terminate any active TTU subprocess and clear the connection."""
        if self.conn:
            if self.conn.get("sp") and self.conn["sp"].poll() is None:
                self.conn["sp"].terminate()
                try:
                    self.conn["sp"].wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.log.warning("Subprocess did not terminate in time. Forcing kill...")
                    self.conn["sp"].kill()
            self.conn = None
