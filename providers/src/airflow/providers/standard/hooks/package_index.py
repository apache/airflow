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
"""Hook for additional Package Indexes (Python)."""

from __future__ import annotations

import subprocess
from typing import Any
from urllib.parse import quote, urlparse

from airflow.hooks.base import BaseHook


class PackageIndexHook(BaseHook):
    """Specify package indexes/Python package sources using Airflow connections."""

    conn_name_attr = "pi_conn_id"
    default_conn_name = "package_index_default"
    conn_type = "package_index"
    hook_name = "Package Index (Python)"

    def __init__(self, pi_conn_id: str = default_conn_name, **kwargs) -> None:
        super().__init__(**kwargs)
        self.pi_conn_id = pi_conn_id
        self.conn = None

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {"host": "Package Index URL"},
            "placeholders": {
                "host": "Example: https://my-package-mirror.net/pypi/repo-name/simple",
                "login": "Username for package index",
                "password": "Password for package index (will be masked)",
            },
        }

    @staticmethod
    def _get_basic_auth_conn_url(index_url: str, user: str | None, password: str | None) -> str:
        """Return a connection URL with basic auth credentials based on connection config."""
        url = urlparse(index_url)
        host = url.netloc.split("@")[-1]
        if user:
            if password:
                host = f"{quote(user)}:{quote(password)}@{host}"
            else:
                host = f"{quote(user)}@{host}"
        return url._replace(netloc=host).geturl()

    def get_conn(self) -> Any:
        """Return connection for the hook."""
        return self.get_connection_url()

    def get_connection_url(self) -> Any:
        """Return a connection URL with embedded credentials."""
        conn = self.get_connection(self.pi_conn_id)
        index_url = conn.host
        if not index_url:
            raise ValueError("Please provide an index URL.")
        return self._get_basic_auth_conn_url(index_url, conn.login, conn.password)

    def test_connection(self) -> tuple[bool, str]:
        """Test connection to package index url."""
        conn_url = self.get_connection_url()
        proc = subprocess.run(
            ["pip", "search", "not-existing-test-package", "--no-input", "--index", conn_url],
            check=False,
            capture_output=True,
        )
        conn = self.get_connection(self.pi_conn_id)
        if proc.returncode not in [
            0,  # executed successfully, found package
            23,  # executed successfully, didn't find any packages
            #      (but we do not expect it to find 'not-existing-test-package')
        ]:
            return False, f"Connection test to {conn.host} failed. Error: {str(proc.stderr)}"

        return True, f"Connection to {conn.host} tested successfully!"
