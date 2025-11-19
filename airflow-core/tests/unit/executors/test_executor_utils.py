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

from airflow.executors.executor_constants import LOCAL_EXECUTOR, ConnectorSource
from airflow.executors.executor_loader import ExecutorLoader, ExecutorName

CORE_EXEC_ALIAS = LOCAL_EXECUTOR
CORE_EXEC_MODULE_PATH = ExecutorLoader.executors[CORE_EXEC_ALIAS]
CORE_EXEC_TEAM_NAME = "team_a"
CUSTOM_EXEC_MODULE_PATH = "custom.module.path"
CUSTOM_EXEC_ALIAS = "custom_executor"
CUSTOM_EXEC_TEAM_NAME = "team_b"


class TestExecutorName:
    @pytest.fixture
    def core_executor(self):
        return ExecutorName(alias=CORE_EXEC_ALIAS, module_path=CORE_EXEC_MODULE_PATH)

    @pytest.fixture
    def core_executor_team_name(self):
        return ExecutorName(
            alias=CORE_EXEC_ALIAS, module_path=CORE_EXEC_MODULE_PATH, team_name=CORE_EXEC_TEAM_NAME
        )

    @pytest.fixture
    def custom_executor(self):
        return ExecutorName(module_path=CUSTOM_EXEC_MODULE_PATH)

    @pytest.fixture
    def custom_executor_alias(self):
        return ExecutorName(module_path=CUSTOM_EXEC_MODULE_PATH, alias=CUSTOM_EXEC_ALIAS)

    @pytest.fixture
    def custom_executor_team_name(self):
        return ExecutorName(module_path=CUSTOM_EXEC_MODULE_PATH, team_name=CUSTOM_EXEC_TEAM_NAME)

    @pytest.fixture
    def custom_executor_team_name_alias(self):
        return ExecutorName(
            module_path=CUSTOM_EXEC_MODULE_PATH, alias=CUSTOM_EXEC_ALIAS, team_name=CUSTOM_EXEC_TEAM_NAME
        )

    def test_initialization(
        self,
        core_executor,
        core_executor_team_name,
        custom_executor,
        custom_executor_team_name,
        custom_executor_alias,
        custom_executor_team_name_alias,
    ):
        assert core_executor.module_path == CORE_EXEC_MODULE_PATH
        assert core_executor.alias is CORE_EXEC_ALIAS
        assert core_executor.team_name is None
        assert core_executor.connector_source == ConnectorSource.CORE

        assert core_executor_team_name.module_path == CORE_EXEC_MODULE_PATH
        assert core_executor_team_name.alias is CORE_EXEC_ALIAS
        assert core_executor_team_name.team_name == CORE_EXEC_TEAM_NAME
        assert core_executor_team_name.connector_source == ConnectorSource.CORE

        assert custom_executor.module_path == CUSTOM_EXEC_MODULE_PATH
        assert custom_executor.alias is None
        assert custom_executor.team_name is None
        assert custom_executor.connector_source == ConnectorSource.CUSTOM_PATH

        assert custom_executor_team_name.module_path == CUSTOM_EXEC_MODULE_PATH
        assert custom_executor_team_name.alias is None
        assert custom_executor_team_name.team_name == CUSTOM_EXEC_TEAM_NAME
        assert custom_executor_team_name.connector_source == ConnectorSource.CUSTOM_PATH

        assert custom_executor_alias.module_path == CUSTOM_EXEC_MODULE_PATH
        assert custom_executor_alias.alias == CUSTOM_EXEC_ALIAS
        assert custom_executor_alias.team_name is None
        assert custom_executor_alias.connector_source == ConnectorSource.CUSTOM_PATH

        assert custom_executor_team_name_alias.module_path == CUSTOM_EXEC_MODULE_PATH
        assert custom_executor_team_name_alias.alias == CUSTOM_EXEC_ALIAS
        assert custom_executor_team_name_alias.team_name == CUSTOM_EXEC_TEAM_NAME
        assert custom_executor_team_name_alias.connector_source == ConnectorSource.CUSTOM_PATH

    def test_repr_all(self, core_executor, core_executor_team_name, custom_executor_team_name_alias):
        assert repr(core_executor) == f":{CORE_EXEC_ALIAS}:"
        assert repr(core_executor_team_name) == f"{CORE_EXEC_TEAM_NAME}:{CORE_EXEC_ALIAS}:"
        assert (
            repr(custom_executor_team_name_alias)
            == f"{CUSTOM_EXEC_TEAM_NAME}:{CUSTOM_EXEC_ALIAS}:{CUSTOM_EXEC_MODULE_PATH}"
        )

    def test_eq_same(self, core_executor_team_name):
        compare_exec = ExecutorName(
            alias=CORE_EXEC_ALIAS, module_path=CORE_EXEC_MODULE_PATH, team_name=CORE_EXEC_TEAM_NAME
        )

        assert core_executor_team_name == compare_exec

    def test_eq_different(self, core_executor, core_executor_team_name, custom_executor_team_name):
        assert core_executor != core_executor_team_name
        assert core_executor_team_name != custom_executor_team_name

    def test_hash_same(self, core_executor_team_name):
        compare_exec = ExecutorName(
            alias=CORE_EXEC_ALIAS, module_path=CORE_EXEC_MODULE_PATH, team_name=CORE_EXEC_TEAM_NAME
        )
        assert hash(core_executor_team_name) == hash(compare_exec)

    def test_hash_different(self, core_executor, core_executor_team_name, custom_executor_team_name_alias):
        assert hash(core_executor) != hash(core_executor_team_name)
        assert hash(core_executor_team_name) != hash(custom_executor_team_name_alias)
