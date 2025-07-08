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

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("Human in the loop public API compatible with Airflow >= 3.0.1", allow_module_level=True)

from sqlalchemy import select

from airflow.models import Trigger
from airflow.models.hitl import HITLResponseModel
from airflow.providers.standard.operators.hitl import (
    ApprovalOperator,
    HITLEntryOperator,
    HITLOperator,
    HITLTerminationOperator,
)
from airflow.providers.standard.triggers.hitl import HITLTriggerEventSuccessPayload
from airflow.sdk import Param
from airflow.sdk.definitions.param import ParamsDict

pytestmark = pytest.mark.db_test


class TestHITLOperator:
    def test_validate_defaults(self) -> None:
        hitl_op = HITLOperator(
            task_id="hitl_test",
            subject="This is subject",
            options=["1", "2", "3", "4", "5"],
            body="This is body",
            default=["1"],
            multiple=False,
            params=ParamsDict({"input_1": 1}),
        )
        hitl_op.validate_defaults()

    @pytest.mark.parametrize(
        "extra_kwargs",
        [
            {"default": None, "execution_timeout": 10},
            {"default": ["0"]},
            {"multiple": False, "default": ["1", "2"]},
        ],
        ids=["timeout with no default", "default not in option", "multiple default when multiple is False"],
    )
    def test_validate_defaults_with_invalid_defaults(self, extra_kwargs) -> None:
        with pytest.raises(ValueError):
            HITLOperator(
                task_id="hitl_test",
                subject="This is subject",
                body="This is body",
                options=["1", "2", "3", "4", "5"],
                params=ParamsDict({"input_1": 1}),
                **extra_kwargs,
            )

    def test_execute(self, dag_maker, session) -> None:
        with dag_maker("test_dag"):
            task = HITLOperator(
                task_id="hitl_test",
                subject="This is subject",
                options=["1", "2", "3", "4", "5"],
                body="This is body",
                default=["1"],
                multiple=False,
                params=ParamsDict({"input_1": 1}),
            )
        dr = dag_maker.create_dagrun()
        ti = dag_maker.run_ti(task.task_id, dr)

        hitl_response_model = session.scalar(
            select(HITLResponseModel).where(HITLResponseModel.ti_id == ti.id)
        )
        assert hitl_response_model.ti_id == ti.id
        assert hitl_response_model.subject == "This is subject"
        assert hitl_response_model.options == ["1", "2", "3", "4", "5"]
        assert hitl_response_model.body == "This is body"
        assert hitl_response_model.default == ["1"]
        assert hitl_response_model.multiple is False
        assert hitl_response_model.params == {"input_1": 1}
        assert hitl_response_model.response_at is None
        assert hitl_response_model.user_id is None
        assert hitl_response_model.response_content is None
        assert hitl_response_model.params_input == {}

        registered_trigger = session.scalar(
            select(Trigger).where(Trigger.classpath == "airflow.providers.standard.triggers.hitl.HITLTrigger")
        )
        assert registered_trigger.kwargs == {
            "ti_id": ti.id,
            "options": ["1", "2", "3", "4", "5"],
            "default": ["1"],
            "params": {"input_1": 1},
            "multiple": False,
            "timeout_datetime": None,
            "poke_interval": 5.0,
        }

    @pytest.mark.parametrize(
        "input_params, expected_params",
        [
            (ParamsDict({"input": 1}), {"input": 1}),
            ({"input": Param(5, type="integer", minimum=3)}, {"input": 5}),
            (None, {}),
        ],
    )
    def test_serialzed_params(self, input_params, expected_params) -> None:
        hitl_op = HITLOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            options=["1", "2", "3", "4", "5"],
            params=input_params,
        )
        assert hitl_op.serialzed_params == expected_params

    def test_execute_complete(self) -> None:
        hitl_op = HITLOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            options=["1", "2", "3", "4", "5"],
            params={"input": 1},
        )

        ret = hitl_op.execute_complete(
            context={},
            event=HITLTriggerEventSuccessPayload(response_content=["1"], params_input={"input": 2}),
        )

        assert ret["response_content"] == ["1"]
        assert ret["params_input"] == {"input": 2}

    def test_validate_response_content_with_invalid_content(self) -> None:
        hitl_op = HITLOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            options=["1", "2", "3", "4", "5"],
            params={"input": 1},
        )

        with pytest.raises(ValueError):
            hitl_op.execute_complete(
                context={},
                event=HITLTriggerEventSuccessPayload(
                    response_content=["not exists"],
                    params_input={"input": 2},
                ),
            )

    def test_validate_params_input_with_invalid_input(self) -> None:
        hitl_op = HITLOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            options=["1", "2", "3", "4", "5"],
            params={"input": 1},
        )

        with pytest.raises(ValueError):
            hitl_op.execute_complete(
                context={},
                event=HITLTriggerEventSuccessPayload(
                    response_content=["1"],
                    params_input={"no such key": 2, "input": 333},
                ),
            )


class TestApprovalOperator:
    def test_init(self):
        with pytest.raises(ValueError):
            ApprovalOperator(
                task_id="hitl_test",
                subject="This is subject",
                body="This is body",
                options=["1", "2", "3", "4", "5"],
                params={"input": 1},
            )


class TestHITLTerminationOperator:
    def test_init(self):
        with pytest.raises(ValueError):
            HITLTerminationOperator(
                task_id="hitl_test",
                subject="This is subject",
                body="This is body",
                options=["1", "2", "3", "4", "5"],
                params={"input": 1},
            )


class TestHITLEntryOperator:
    def test_init(self):
        op = HITLEntryOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            params={"input": 1},
        )

        assert op.options == ["OK"]
        assert op.default == ["OK"]
