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
    pytest.skip("Human in the loop is only compatible with Airflow >= 3.1.0", allow_module_level=True)

import datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from airflow.exceptions import DownstreamTasksSkipped
from airflow.models import TaskInstance, Trigger
from airflow.models.hitl import HITLDetail
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import (
    ApprovalOperator,
    HITLBranchOperator,
    HITLEntryOperator,
    HITLOperator,
)
from airflow.sdk import Param, timezone
from airflow.sdk.definitions.param import ParamsDict

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from tests_common.pytest_plugin import DagMaker

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
INTERVAL = datetime.timedelta(hours=12)


@pytest.fixture
def hitl_task_and_ti_for_generating_link(dag_maker: DagMaker) -> tuple[HITLOperator, TaskInstance]:
    with dag_maker("test_dag"):
        task = HITLOperator(
            task_id="hitl_test",
            subject="This is subject",
            options=["1", "2", "3", "4", "5"],
            body="This is body",
            defaults=["1"],
            respondents="test",
            multiple=True,
            params=ParamsDict({"input_1": 1, "input_2": 2, "input_3": 3}),
        )
    dr = dag_maker.create_dagrun()
    return task, dag_maker.run_ti(task.task_id, dr)


class TestHITLOperator:
    def test_validate_options(self) -> None:
        hitl_op = HITLOperator(
            task_id="hitl_test",
            subject="This is subject",
            options=["1", "2", "3", "4", "5"],
            body="This is body",
            defaults=["1"],
            multiple=False,
            params=ParamsDict({"input_1": 1}),
        )
        hitl_op.validate_options()

    @pytest.mark.parametrize(
        "options, expected_err_msg",
        [
            ([], '"options" cannot be empty.'),
            (["1,1", "1", "2"], '"," is not allowed in option'),
        ],
        ids=["empty", "comma"],
    )
    def test_validate_options_with_empty_options(self, options: list[str], expected_err_msg: str) -> None:
        # validate_options is called during initialization
        with pytest.raises(ValueError, match=expected_err_msg):
            HITLOperator(
                task_id="hitl_test",
                subject="This is subject",
                options=options,
                body="This is body",
                defaults=["1"],
                multiple=False,
                params=ParamsDict({"input_1": 1}),
            )

    def test_validate_params_with__options(self) -> None:
        # validate_params is called during initialization
        with pytest.raises(ValueError, match='"_options" is not allowed in params'):
            HITLOperator(
                task_id="hitl_test",
                subject="This is subject",
                options=["1", "2"],
                body="This is body",
                defaults=["1"],
                multiple=False,
                params=ParamsDict({"_options": 1}),
            )

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
        # validate_default is called during initialization
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
        notifier = MagicMock()

        with dag_maker("test_dag"):
            task = HITLOperator(
                task_id="hitl_test",
                subject="This is subject",
                options=["1", "2", "3", "4", "5"],
                body="This is body",
                defaults=["1"],
                respondents="test",
                multiple=False,
                params=ParamsDict({"input_1": 1}),
                notifiers=[notifier],
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
        assert hitl_detail_model.respondents == ["test"]
        assert hitl_detail_model.response_at is None
        assert hitl_detail_model.user_id is None
        assert hitl_detail_model.chosen_options is None
        assert hitl_detail_model.params_input == {}

        assert notifier.called is True

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
        assert hitl_op.serialized_params == expected_params

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

    @pytest.mark.parametrize(
        "options, params_input, expected_query_string",
        [
            (None, None, "?map_index=-1"),
            ("1", None, "?_options=1&map_index=-1"),
            (["1", "2"], None, "?_options=1%2C2&map_index=-1"),
            (None, {"input_1": 123}, "?input_1=123&map_index=-1"),
            (
                ["3", "4", "5"],
                {"input_1": 123123, "input_2": 345345},
                "?_options=3%2C4%2C5&input_1=123123&input_2=345345&map_index=-1",
            ),
        ],
        ids=[
            "empty",
            "single-option",
            "multiple-options",
            "single-param-input",
            "multiple-options-and-param-inputs",
        ],
    )
    @pytest.mark.parametrize("base_url", ["http://test", "http://test_2:8080"])
    def test_generate_link_to_ui(
        self,
        base_url: str,
        options: list[str] | None,
        params_input: dict[str, Any] | None,
        expected_query_string: str,
        hitl_task_and_ti_for_generating_link: tuple[HITLOperator, TaskInstance],
    ) -> None:
        expected_url = (
            f"{base_url}/dags/test_dag/runs/test/tasks/hitl_test/required_actions{expected_query_string}"
        )
        task, ti = hitl_task_and_ti_for_generating_link
        url = task.generate_link_to_ui(
            task_instance=ti,
            base_url=base_url,
            options=options,
            params_input=params_input,
        )
        assert url == expected_url

    @pytest.mark.parametrize(
        "options, params_input, expected_query_string",
        [
            (None, None, "?map_index=-1"),
            ("1", None, "?_options=1&map_index=-1"),
            (["1", "2"], None, "?_options=1%2C2&map_index=-1"),
            (None, {"input_1": 123}, "?input_1=123&map_index=-1"),
            (
                ["3", "4", "5"],
                {"input_1": 123123, "input_2": 345345},
                "?_options=3%2C4%2C5&input_1=123123&input_2=345345&map_index=-1",
            ),
        ],
        ids=[
            "empty",
            "single-option",
            "multiple-options",
            "single-param-input",
            "multiple-options-and-param-inputs",
        ],
    )
    @conf_vars({("api", "base_url"): "http://localhost:8080/"})
    def test_generate_link_fall_back_to_conf_api_base_url(
        self,
        options: list[str] | None,
        params_input: dict[str, Any] | None,
        expected_query_string: str,
        hitl_task_and_ti_for_generating_link: tuple[HITLOperator, TaskInstance],
    ) -> None:
        task, ti = hitl_task_and_ti_for_generating_link
        expected_url = f"http://localhost:8080/dags/test_dag/runs/test/tasks/hitl_test/required_actions{expected_query_string}"

        url = task.generate_link_to_ui(
            task_instance=ti,
            options=options,
            params_input=params_input,
        )
        assert url == expected_url

    @pytest.mark.parametrize(
        "options, params_input, expected_err_msg",
        [
            ([100, "2", 30000], None, "options {.*} are not valid options"),
            (
                None,
                {"input_not_exist": 123, "no_such_key": 123},
                "params {.*} are not valid params",
            ),
        ],
    )
    def test_generate_link_to_ui_with_invalid_input(
        self,
        options: list[str] | None,
        params_input: dict[str, Any] | None,
        expected_err_msg: str,
        hitl_task_and_ti_for_generating_link: tuple[HITLOperator, TaskInstance],
    ) -> None:
        task, ti = hitl_task_and_ti_for_generating_link
        with pytest.raises(ValueError, match=expected_err_msg):
            task.generate_link_to_ui(task_instance=ti, options=options, params_input=params_input)

    def test_generate_link_to_ui_without_base_url(
        self,
        hitl_task_and_ti_for_generating_link: tuple[HITLOperator, TaskInstance],
    ) -> None:
        task, ti = hitl_task_and_ti_for_generating_link
        with pytest.raises(ValueError, match="Not able to retrieve base_url"):
            task.generate_link_to_ui(task_instance=ti)


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
    def test_init_without_options_and_default(self) -> None:
        op = HITLEntryOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            params={"input": 1},
        )

        assert op.options == ["OK"]
        assert op.defaults == ["OK"]

    def test_init_without_options(self) -> None:
        op = HITLEntryOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            params={"input": 1},
            defaults=None,
        )

        assert op.options == ["OK"]
        assert op.defaults is None

    def test_init_without_default(self) -> None:
        op = HITLEntryOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            params={"input": 1},
            options=["OK", "NOT OK"],
        )

        assert op.options == ["OK", "NOT OK"]
        assert op.defaults is None


class TestHITLBranchOperator:
    def test_execute_complete(self, dag_maker) -> None:
        with dag_maker("hitl_test_dag", serialized=True):
            branch_op = HITLBranchOperator(
                task_id="make_choice",
                subject="This is subject",
                options=[f"branch_{i}" for i in range(1, 6)],
            )

            branch_op >> [EmptyOperator(task_id=f"branch_{i}") for i in range(1, 6)]

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("make_choice")
        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            branch_op.execute_complete(
                context={"ti": ti, "task": ti.task},
                event={
                    "chosen_options": ["branch_1"],
                    "params_input": {},
                },
            )
        assert set(exc_info.value.tasks) == set((f"branch_{i}", -1) for i in range(2, 6))

    def test_execute_complete_with_multiple_branches(self, dag_maker) -> None:
        with dag_maker("hitl_test_dag", serialized=True):
            branch_op = HITLBranchOperator(
                task_id="make_choice",
                subject="This is subject",
                multiple=True,
                options=[f"branch_{i}" for i in range(1, 6)],
            )

            branch_op >> [EmptyOperator(task_id=f"branch_{i}") for i in range(1, 6)]

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("make_choice")
        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            branch_op.execute_complete(
                context={"ti": ti, "task": ti.task},
                event={
                    "chosen_options": [f"branch_{i}" for i in range(1, 4)],
                    "params_input": {},
                },
            )
        assert set(exc_info.value.tasks) == set((f"branch_{i}", -1) for i in range(4, 6))
