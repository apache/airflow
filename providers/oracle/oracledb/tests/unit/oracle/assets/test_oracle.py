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

from airflow.providers.oracle.oracledb.assets import oracle as oracledb_asset
from airflow.utils.deprecation_tools import DeprecatedImportWarning


class TestDeprecatedAssetsImport:
    """`airflow.providers.oracle.assets.oracle` is deprecated; it must keep re-exporting
    the real functions from `airflow.providers.oracle.oracledb.assets.oracle` unchanged."""

    def test_sanitize_uri_redirects_and_warns(self):
        import airflow.providers.oracle.assets.oracle as deprecated_module

        with warnings.catch_warnings(record=True) as captured_warnings:
            warnings.simplefilter("always")
            sanitize_uri = deprecated_module.sanitize_uri

        assert sanitize_uri is oracledb_asset.sanitize_uri
        assert any(issubclass(w.category, DeprecatedImportWarning) for w in captured_warnings)

    def test_create_asset_redirects_and_warns(self):
        import airflow.providers.oracle.assets.oracle as deprecated_module

        with warnings.catch_warnings(record=True) as captured_warnings:
            warnings.simplefilter("always")
            create_asset = deprecated_module.create_asset

        assert create_asset is oracledb_asset.create_asset
        assert any(issubclass(w.category, DeprecatedImportWarning) for w in captured_warnings)

    def test_convert_asset_to_openlineage_redirects_and_warns(self):
        import airflow.providers.oracle.assets.oracle as deprecated_module

        with warnings.catch_warnings(record=True) as captured_warnings:
            warnings.simplefilter("always")
            convert_asset_to_openlineage = deprecated_module.convert_asset_to_openlineage

        assert convert_asset_to_openlineage is oracledb_asset.convert_asset_to_openlineage
        assert any(issubclass(w.category, DeprecatedImportWarning) for w in captured_warnings)
