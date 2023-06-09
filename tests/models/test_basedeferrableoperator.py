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

import importlib
from unittest import mock

import pytest

from airflow.models import basedeferrableoperator


class TestDeferrableMixin:
    @pytest.mark.parametrize(
        "default_deferrable, expected_deferrable",
        [
            ("true", True),
            ("false", False),
        ],
    )
    def test_init(self, default_deferrable: str, expected_deferrable: bool) -> None:
        with mock.patch.dict(
            "os.environ",
            {
                "AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE": default_deferrable,
            },
        ):
            importlib.reload(basedeferrableoperator)
            deferrable_operator = basedeferrableoperator.BaseDeferrableOperator(task_id="deferrable_operator")
            result_deferrable = deferrable_operator.deferrable
            assert result_deferrable == expected_deferrable
