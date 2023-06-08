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

from airflow.models import deferrablemixin
from airflow.models.baseoperator import BaseOperator


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
            importlib.reload(deferrablemixin)
            result_deferrable = deferrablemixin.DeferrableMixin().deferrable
            assert result_deferrable == expected_deferrable

    @pytest.mark.parametrize(
        "default_deferrable, expected_deferrable",
        [
            ("true", True),
            ("false", False),
        ],
    )
    def test_with_pseudo_operator(self, default_deferrable: str, expected_deferrable: bool) -> None:
        class DeferrableOperator(BaseOperator, deferrablemixin.DeferrableMixin):
            pass

        with mock.patch.dict(
            "os.environ",
            {
                "AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE": default_deferrable,
            },
        ):
            importlib.reload(deferrablemixin)

            deferrable_operator = DeferrableOperator(task_id="defererable_operator")
            assert deferrable_operator.deferrable == expected_deferrable
