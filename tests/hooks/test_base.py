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

from airflow.hooks.base import BaseHook


class TestBaseHook:
    def test_hook_has_default_logger_name(self):
        hook = BaseHook()
        assert hook.log.name == "airflow.task.hooks.airflow.hooks.base.BaseHook"

    def test_custom_logger_name_is_correctly_set(self):
        hook = BaseHook(logger_name="airflow.custom.logger")
        assert hook.log.name == "airflow.task.hooks.airflow.custom.logger"

    def test_empty_string_as_logger_name(self):
        hook = BaseHook(logger_name="")
        assert hook.log.name == "airflow.task.hooks"
