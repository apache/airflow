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

from airflow.providers.oracle.oracledb.transfers.oracle_to_oracle import (
    OracleToOracleOperator as OracleDbOracleToOracleOperator,
)
from airflow.utils.deprecation_tools import DeprecatedImportWarning


class TestDeprecatedOracleToOracleOperatorImport:
    """`airflow.providers.oracle.transfers.oracle_to_oracle` is deprecated; it must keep
    re-exporting the real class from `airflow.providers.oracle.oracledb.transfers.oracle_to_oracle`
    unchanged."""

    def test_attribute_access_redirects_to_oracledb_and_warns(self):
        import airflow.providers.oracle.transfers.oracle_to_oracle as deprecated_module

        with warnings.catch_warnings(record=True) as captured_warnings:
            warnings.simplefilter("always")
            operator_cls = deprecated_module.OracleToOracleOperator

        assert operator_cls is OracleDbOracleToOracleOperator
        assert any(issubclass(w.category, DeprecatedImportWarning) for w in captured_warnings)

    def test_from_import_redirects_to_oracledb_and_warns(self):
        with warnings.catch_warnings(record=True) as captured_warnings:
            warnings.simplefilter("always")
            from airflow.providers.oracle.transfers.oracle_to_oracle import OracleToOracleOperator

        assert OracleToOracleOperator is OracleDbOracleToOracleOperator
        assert any(issubclass(w.category, DeprecatedImportWarning) for w in captured_warnings)
