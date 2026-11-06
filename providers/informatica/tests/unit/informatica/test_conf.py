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

from unittest.mock import patch

from airflow.providers.informatica.conf import auto_lineage_enabled, disabled_operators, is_operator_disabled


class SomeOperator:
    pass


class TestDisabledOperators:
    def test_empty_config_returns_empty_set(self):
        with patch("airflow.providers.informatica.conf.conf") as mock_conf:
            mock_conf.get.return_value = ""
            assert disabled_operators() == set()

    def test_single_fqcn(self):
        with patch("airflow.providers.informatica.conf.conf") as mock_conf:
            mock_conf.get.return_value = "airflow.operators.bash.BashOperator"
            assert disabled_operators() == {"airflow.operators.bash.BashOperator"}

    def test_multiple_fqcns_semicolon_separated(self):
        with patch("airflow.providers.informatica.conf.conf") as mock_conf:
            mock_conf.get.return_value = (
                "airflow.operators.bash.BashOperator;airflow.operators.python.PythonOperator"
            )
            result = disabled_operators()
            assert "airflow.operators.bash.BashOperator" in result
            assert "airflow.operators.python.PythonOperator" in result

    def test_whitespace_around_entries_is_stripped(self):
        with patch("airflow.providers.informatica.conf.conf") as mock_conf:
            mock_conf.get.return_value = "  foo.Bar ; baz.Qux  "
            assert disabled_operators() == {"foo.Bar", "baz.Qux"}

    def test_trailing_semicolon_ignored(self):
        with patch("airflow.providers.informatica.conf.conf") as mock_conf:
            mock_conf.get.return_value = "foo.Bar;"
            assert disabled_operators() == {"foo.Bar"}


class TestAutoLineageEnabled:
    def test_defaults_to_false(self):
        with patch("airflow.providers.informatica.conf.conf") as mock_conf:
            mock_conf.getboolean.return_value = False
            assert auto_lineage_enabled() is False

    def test_returns_true_when_configured(self):
        with patch("airflow.providers.informatica.conf.conf") as mock_conf:
            mock_conf.getboolean.return_value = True
            assert auto_lineage_enabled() is True


class TestIsOperatorDisabled:
    def test_matching_fqcn_returns_true(self):
        fqcn = f"{SomeOperator.__module__}.{SomeOperator.__name__}"
        with patch("airflow.providers.informatica.conf.conf") as mock_conf:
            mock_conf.get.return_value = fqcn
            assert is_operator_disabled(SomeOperator()) is True

    def test_non_matching_fqcn_returns_false(self):
        with patch("airflow.providers.informatica.conf.conf") as mock_conf:
            mock_conf.get.return_value = "other.Operator"
            assert is_operator_disabled(SomeOperator()) is False

    def test_empty_disabled_list_returns_false(self):
        with patch("airflow.providers.informatica.conf.conf") as mock_conf:
            mock_conf.get.return_value = ""
            assert is_operator_disabled(SomeOperator()) is False
