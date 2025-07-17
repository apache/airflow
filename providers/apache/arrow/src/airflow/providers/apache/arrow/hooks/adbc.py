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

import functools
from functools import cached_property
from typing import Any

from adbc_driver_manager.dbapi import connect, Connection

from airflow.providers.common.sql.hooks.sql import DbApiHook


# https://arrow.apache.org/adbc/current/python/api/adbc_driver_manager.html
# https://arrow.apache.org/docs/python/
class AdbcHook(DbApiHook):
    """
    General hook for ADBC access.
    """

    conn_name_attr = "adbc_conn_id"
    default_conn_name = "adbc_default"
    conn_type = "adbc"
    hook_name = "ADBC Connection"
    supports_autocommit = True

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Get custom field behaviour."""
        return {
            "hidden_fields": ["port", "schema"],
            "relabeling": {"host": "Connection URL"},
        }

    @functools.lru_cache
    def _driver_path(self) -> str | None:
        import pathlib
        import sys

        import importlib_resources

        driver = self.connection_extra_lower.get("driver")

        if driver:
            # Wheels bundle the shared library
            root = importlib_resources.files(driver)
            # The filename is always the same regardless of platform
            entrypoint = root.joinpath(f"lib{driver}.so")
            if entrypoint.is_file():
                return str(entrypoint)

            # Search sys.prefix + '/lib' (Unix, Conda on Unix)
            root = pathlib.Path(sys.prefix)
            for filename in (f"lib{driver}.so", f"lib{driver}.dylib"):
                entrypoint = root.joinpath("lib", filename)
                if entrypoint.is_file():
                    return str(entrypoint)

            # Conda on Windows
            entrypoint = root.joinpath("bin", f"{driver}.dll")
            if entrypoint.is_file():
                return str(entrypoint)

            # Let the driver manager fall back to (DY)LD_LIBRARY_PATH/PATH
            # (It will insert 'lib', 'so', etc. as needed)
            return driver
        return None

    @cached_property
    def uri(self) -> str:
        return self.connection.host

    def get_conn(self) -> Connection:
        return connect(
            driver=self._driver_path(),
            #entrypoint: Optional[str] = None,
            db_kwargs={"uri": self.uri},
            #conn_kwargs: Optional[Dict[str, str]] = None,
            autocommit=False,
        )
