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

import warnings

from airflow.providers.oracle.oracledb.hooks import handlers as oracledb_handlers
from airflow.utils.deprecation_tools import DeprecatedImportWarning


class TestDeprecatedHandlersImport:
    """`airflow.providers.oracle.hooks.handlers` is deprecated; it must keep re-exporting
    the real functions from `airflow.providers.oracle.oracledb.hooks.handlers` unchanged."""

    def test_fetch_all_handler_redirects_and_warns(self):
        import airflow.providers.oracle.hooks.handlers as deprecated_module

        with warnings.catch_warnings(record=True) as captured_warnings:
            warnings.simplefilter("always")
            fetch_all_handler = deprecated_module.fetch_all_handler

        assert fetch_all_handler is oracledb_handlers.fetch_all_handler
        assert any(issubclass(w.category, DeprecatedImportWarning) for w in captured_warnings)

    def test_fetch_one_handler_redirects_and_warns(self):
        import airflow.providers.oracle.hooks.handlers as deprecated_module

        with warnings.catch_warnings(record=True) as captured_warnings:
            warnings.simplefilter("always")
            fetch_one_handler = deprecated_module.fetch_one_handler

        assert fetch_one_handler is oracledb_handlers.fetch_one_handler
        assert any(issubclass(w.category, DeprecatedImportWarning) for w in captured_warnings)
