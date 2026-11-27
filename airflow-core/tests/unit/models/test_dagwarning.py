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

import pytest

from airflow.models.dagwarning import DagWarning, DagWarningType, get_warning_type_value


class TestGetWarningTypeValue:
    @pytest.mark.parametrize(
        ("warning_type", "expected"),
        [
            pytest.param(DagWarningType.NONEXISTENT_POOL, "non-existent pool", id="enum-member"),
            pytest.param("non-existent pool", "non-existent pool", id="built-in-string"),
            pytest.param("yaml:schema_violation", "yaml:schema_violation", id="importer-type"),
            pytest.param(f"yaml:{'x' * 45}", f"yaml:{'x' * 45}", id="importer-type-at-column-length"),
        ],
    )
    def test_valid_type_returns_stored_value(self, warning_type, expected):
        assert get_warning_type_value(warning_type) == expected

    @pytest.mark.parametrize(
        "warning_type",
        [
            pytest.param(1, id="non-string"),
            pytest.param("yaml schema violation", id="no-namespace"),
            pytest.param("non existent pool", id="mistyped-built-in-type"),
            pytest.param("YAML:Schema_Violation", id="uppercase"),
            pytest.param(":schema_violation", id="empty-namespace"),
            pytest.param("yaml:", id="empty-type"),
            pytest.param(f"yaml:{'x' * 46}", id="longer-than-column"),
        ],
    )
    def test_invalid_type_is_rejected(self, warning_type):
        with pytest.raises(ValueError, match="validation error"):
            get_warning_type_value(warning_type)

    def test_dag_warning_stores_importer_type(self):
        assert DagWarning("dag_1", "yaml:schema_violation", "message").warning_type == "yaml:schema_violation"
