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

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from airflow.exceptions import DownstreamTasksSkipped
from airflow.models import Trigger
from airflow.models.hitl import HITLDetail
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import (
    ApprovalOperator,
    HITLEntryOperator,
    HITLOperator,
)
from airflow.sdk import Param
from airflow.sdk.definitions.param import ParamsDict

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from tests_common.pytest_plugin import DagMaker

pytestmark = pytest.mark.db_test


class TestHITLOperator:
    def test_validate_defaults(self) -> None:
        hitl_op = HITLOperator(
            task_id="hitl_test",
            subject="This is subject",
            options=["1", "2", "3", "4", "5"],
            body="This is body",
            defaults=["1"],
            multiple=False,
            params=ParamsDict({"input_1": 1}),
        )
        hitl_op.validate_defaults()

    @pytest.mark.parametrize(
        "extra_kwargs, expected_error_msg",
        [
            ({"defaults": ["0"]}, r'defaults ".*" should be a subset of options ".*"'),
            (
                {"multiple": False, "defaults": ["1", "2"]},
                'More than one defaults given when "multiple" is set to False.',
            ),
        ],
        ids=[
            "defaults not in option",
            "multiple defaults when multiple is False",
        ],
    )
    def test_validate_defaults_with_invalid_defaults(
        self,
        extra_kwargs: dict[str, Any],
        expected_error_msg: str,
    ) -> None:
        with pytest.raises(ValueError, match=expected_error_msg):
            HITLOperator(
                task_id="hitl_test",
                subject="This is subject",
                body="This is body",
                options=["1", "2", "3", "4", "5"],
                params=ParamsDict({"input_1": 1}),
                **extra_kwargs,
            )

    def test_execute(self, dag_maker: DagMaker, session: Session) -> None:
        with dag_maker("test_dag"):
            task = HITLOperator(
                task_id="hitl_test",
                subject="This is subject",
                options=["1", "2", "3", "4", "5"],
                body="This is body",
                defaults=["1"],
                multiple=False,
                params=ParamsDict({"input_1": 1}),
            )
        dr = dag_maker.create_dagrun()
        ti = dag_maker.run_ti(task.task_id, dr)

        hitl_detail_model = session.scalar(select(HITLDetail).where(HITLDetail.ti_id == ti.id))
        assert hitl_detail_model.ti_id == ti.id
        assert hitl_detail_model.subject == "This is subject"
        assert hitl_detail_model.options == ["1", "2", "3", "4", "5"]
        assert hitl_detail_model.body == "This is body"
        assert hitl_detail_model.defaults == ["1"]
        assert hitl_detail_model.multiple is False
        assert hitl_detail_model.params == {"input_1": 1}
        assert hitl_detail_model.response_at is None
        assert hitl_detail_model.user_id is None
        assert hitl_detail_model.chosen_options is None
        assert hitl_detail_model.params_input == {}

        registered_trigger = session.scalar(
            select(Trigger).where(Trigger.classpath == "airflow.providers.standard.triggers.hitl.HITLTrigger")
        )
        assert registered_trigger.kwargs == {
            "ti_id": ti.id,
            "options": ["1", "2", "3", "4", "5"],
            "defaults": ["1"],
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
    def test_serialzed_params(self, input_params, expected_params: dict[str, Any]) -> None:
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
            event={"chosen_options": ["1"], "params_input": {"input": 2}},
        )

        assert ret["chosen_options"] == ["1"]
        assert ret["params_input"] == {"input": 2}

    def test_validate_chosen_options_with_invalid_content(self) -> None:
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
                event={
                    "chosen_options": ["not exists"],
                    "params_input": {"input": 2},
                },
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
                event={
                    "chosen_options": ["1"],
                    "params_input": {"no such key": 2, "input": 333},
                },
            )


class TestApprovalOperator:
    def test_init_with_options(self) -> None:
        with pytest.raises(ValueError):
            ApprovalOperator(
                task_id="hitl_test",
                subject="This is subject",
                body="This is body",
                options=["1", "2", "3", "4", "5"],
                params={"input": 1},
            )

    def test_init_with_multiple_set_to_true(self) -> None:
        with pytest.raises(ValueError):
            ApprovalOperator(
                task_id="hitl_test",
                subject="This is subject",
                params={"input": 1},
                multiple=True,
            )

    def test_execute_complete(self) -> None:
        hitl_op = ApprovalOperator(
            task_id="hitl_test",
            subject="This is subject",
        )

        ret = hitl_op.execute_complete(
            context={},
            event={"chosen_options": ["Approve"], "params_input": {}},
        )

        assert ret == {
            "chosen_options": ["Approve"],
            "params_input": {},
        }

    def test_execute_complete_with_downstream_tasks(self, dag_maker) -> None:
        with dag_maker("hitl_test_dag", serialized=True):
            hitl_op = ApprovalOperator(
                task_id="hitl_test",
                subject="This is subject",
            )
            (hitl_op >> EmptyOperator(task_id="op1"))

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("hitl_test")

        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            hitl_op.execute_complete(
                context={"ti": ti, "task": ti.task},
                event={"chosen_options": ["Reject"], "params_input": {}},
            )
        assert set(exc_info.value.tasks) == {"op1"}


class TestHITLEntryOperator:
    def test_init(self) -> None:
        op = HITLEntryOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            params={"input": 1},
        )

        assert op.options == ["OK"]
        assert op.defaults == ["OK"]
