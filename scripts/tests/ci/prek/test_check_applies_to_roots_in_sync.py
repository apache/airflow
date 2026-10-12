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

import textwrap

import pytest
from check_applies_to_roots_in_sync import extract_python_table, extract_ui_table


class TestExtractPythonTable:
    def test_reads_the_mapping_without_importing_airflow(self):
        source = textwrap.dedent(
            '''
            """Module docstring."""
            _APPLIES_TO_ROOTS: dict[str, frozenset[str]] = {"dag": frozenset({"dag"})}

            _APPLIES_TO_ENTITY_ROOT: dict[str, str] = {
                "dag": "dag",
                "dag_run": "dag_run",
                "task_instance": "task_instance",
            }
            '''
        )

        assert extract_python_table(source) == {
            "dag": "dag",
            "dag_run": "dag_run",
            "task_instance": "task_instance",
        }

    def test_fails_loudly_when_the_table_is_renamed(self):
        with pytest.raises(SystemExit, match="_APPLIES_TO_ENTITY_ROOT"):
            extract_python_table("SOMETHING_ELSE = {}\n")


class TestExtractUiTable:
    def test_reads_the_object_literal(self):
        source = textwrap.dedent(
            """
            type RootName = "dag" | "dagRun";

            const ROOT_BY_PREFIX: Record<string, RootName> = {
              dag: "dag",
            };

            const ENTITY_ROOT_BY_DESTINATION: Record<string, RootName> = {
              dag: "dag",
              dag_run: "dagRun",
              task_instance: "taskInstance",
            };
            """
        )

        assert extract_ui_table(source) == {
            "dag": "dag",
            "dag_run": "dagRun",
            "task_instance": "taskInstance",
        }

    def test_fails_loudly_when_the_table_is_renamed(self):
        with pytest.raises(SystemExit, match="ENTITY_ROOT_BY_DESTINATION"):
            extract_ui_table("const SOMETHING_ELSE: Record<string, RootName> = {\n};\n")


def test_the_checked_in_tables_agree():
    """The check this hook exists for, run against the real files."""
    from check_applies_to_roots_in_sync import main

    assert main() == 0
