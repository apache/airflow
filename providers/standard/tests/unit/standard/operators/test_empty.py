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

from unittest.mock import MagicMock

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.version_compat import BaseOperator


class TestEmptyOperator:
    def test_empty_operator_init(self):
        op = EmptyOperator(task_id="test_empty")
        assert op.task_id == "test_empty"
        assert op.inherits_from_empty_operator is True
        assert op.ui_color == "#e8f7e4"
        assert isinstance(op, BaseOperator)

    def test_empty_operator_execute_none(self):
        op = EmptyOperator(task_id="test_empty_none")
        result = op.execute(None)
        assert result is None

    def test_empty_operator_execute_with_context(self):
        op = EmptyOperator(task_id="test_empty_context")
        mock_context = MagicMock()
        result = op.execute(context=mock_context)
        assert result is None
