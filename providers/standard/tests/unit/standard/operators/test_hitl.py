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

from uuid import UUID, uuid4

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_2_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("Human in the loop is only compatible with Airflow >= 3.1.0", allow_module_level=True)

import datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest
from sqlalchemy import select

from airflow.models import TaskInstance, Trigger
from airflow.models.hitl import HITLDetail
from airflow.providers.common.compat.sdk import AirflowException, DownstreamTasksSkipped, ParamValidationError
from airflow.providers.standard.exceptions import HITLRejectException, HITLTimeoutError, HITLTriggerEventError
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import (
    ApprovalOperator,
    HITLBranchOperator,
    HITLEntryOperator,
    HITLOperator,
)
from airflow.sdk import Param, timezone
from airflow.sdk.definitions.param import ParamsDict
from airflow.sdk.execution_time.hitl import HITLUser

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_3_PLUS

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.sdk import Context
    from airflow.sdk.types import Operator

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
            assigned_users=HITLUser(id="test", name="test"),
            multiple=True,
            params=ParamsDict({"input_1": 1, "input_2": 2, "input_3": 3}),
        )
    dr = dag_maker.create_dagrun()
    return task, dag_maker.run_ti(task.task_id, dr)


@pytest.fixture
def get_context_from_model_ti(mock_supervisor_comms: Any) -> Any:
    def _get_context(ti: TaskInstance, task: Operator) -> Context:
        from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
            DagRun as DRDataModel,
            TaskInstance as TIDataModel,
            TIRunContext,
        )
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

        # make mypy happy
        assert ti is not None

        dag_run = ti.dag_run
        ti_model = TIDataModel.model_validate(ti, from_attributes=True)
        runtime_ti = RuntimeTaskInstance.model_construct(
            **ti_model.model_dump(exclude_unset=True),
            task=task,
            _ti_context_from_server=TIRunContext(
                dag_run=DRDataModel.model_validate(dag_run, from_attributes=True),
                max_tries=ti.max_tries,
                variables=[],
                connections=[],
                xcom_keys_to_clear=[],
            ),
        )
        return runtime_ti.get_template_context()

    return _get_context


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

    def test_validate_options_with_empty_options(self) -> None:
        # validate_options is called during initialization
        with pytest.raises(ValueError, match='"options" cannot be empty.'):
            HITLOperator(
                task_id="hitl_test",
                subject="This is subject",
                options=[],
                body="This is body",
                defaults=["1"],
                multiple=False,
                params=ParamsDict({"input_1": 1}),
            )

    @pytest.mark.parametrize(
        ("params", "exc", "error_msg"),
        (
            (ParamsDict({"_options": 1}), ValueError, '"_options" is not allowed in params'),
            (
                ParamsDict({"param": Param("", type="integer")}),
                ParamValidationError,
                (
                    "Invalid input for param param: '' is not of type 'integer'\n\n"
                    "Failed validating 'type' in schema:\n"
                    "    {'type': 'integer'}\n\n"
                    "On instance:\n    ''"
                ),
            ),
        ),
    )
    def test_validate_params(
        self, params: ParamsDict, exc: type[ValueError | ParamValidationError], error_msg: str
    ) -> None:
        # validate_params is called during initialization
        with pytest.raises(exc, match=error_msg):
            HITLOperator(
                task_id="hitl_test",
                subject="This is subject",
                options=["1", "2"],
                body="This is body",
                defaults=["1"],
                multiple=False,
                params=params,
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
        ("extra_kwargs", "expected_error_msg"),
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
                assigned_users=HITLUser(id="test", name="test"),
                multiple=False,
                params=ParamsDict({"input_1": 1}),
                notifiers=[notifier],
            )
        dr = dag_maker.create_dagrun()
        ti = dag_maker.run_ti(task.task_id, dr)

        hitl_detail_model = session.scalar(select(HITLDetail).where(HITLDetail.ti_id == ti.id))
        assert hitl_detail_model is not None
        assert hitl_detail_model.ti_id == ti.id
        assert hitl_detail_model.subject == "This is subject"
        assert hitl_detail_model.options == ["1", "2", "3", "4", "5"]
        assert hitl_detail_model.body == "This is body"
        assert hitl_detail_model.defaults == ["1"]
        assert hitl_detail_model.multiple is False
        assert hitl_detail_model.assignees == [{"id": "test", "name": "test"}]
        assert hitl_detail_model.responded_at is None
        assert hitl_detail_model.responded_by is None
        assert hitl_detail_model.chosen_options is None
        assert hitl_detail_model.params_input == {}
        expected_params: dict[str, Any]
        if AIRFLOW_V_3_2_PLUS:
            expected_params = {"input_1": {"value": 1, "description": None, "schema": {}, "source": "task"}}
        elif AIRFLOW_V_3_1_3_PLUS:
            expected_params = {"input_1": {"value": 1, "description": None, "schema": {}}}
        else:
            expected_params = {"input_1": 1}
        assert hitl_detail_model.params == expected_params

        assert notifier.called is True

        expected_params_in_trigger_kwargs: dict[str, dict[str, Any]]
        # trigger_kwargs are encoded via BaseSerialization in versions < 3.2
        expected_ti_id: str | UUID = ti.id
        if AIRFLOW_V_3_2_PLUS:
            expected_params_in_trigger_kwargs = expected_params
            # trigger_kwargs are encoded via serde from task sdk in versions >= 3.2
            expected_ti_id = ti.id
        else:
            expected_params_in_trigger_kwargs = {"input_1": {"value": 1, "description": None, "schema": {}}}

        registered_trigger = session.scalar(
            select(Trigger).where(Trigger.classpath == "airflow.providers.standard.triggers.hitl.HITLTrigger")
        )
        assert registered_trigger is not None
        assert registered_trigger.kwargs == {
            "ti_id": expected_ti_id,
            "options": ["1", "2", "3", "4", "5"],
            "defaults": ["1"],
            "params": expected_params_in_trigger_kwargs,
            "multiple": False,
            "timeout_datetime": None,
            "poke_interval": 5.0,
        }

    @pytest.mark.skipif(not AIRFLOW_V_3_1_3_PLUS, reason="This only works in airflow-core >= 3.1.3")
    @pytest.mark.parametrize(
        ("input_params", "expected_params"),
        [
            (
                ParamsDict({"input": 1}),
                {
                    "input": {
                        "description": None,
                        "schema": {},
                        "value": 1,
                    },
                },
            ),
            (
                {"input": Param(5, type="integer", minimum=3, description="test")},
                {
                    "input": {
                        "value": 5,
                        "schema": {
                            "minimum": 3,
                            "type": "integer",
                        },
                        "description": "test",
                    }
                },
            ),
            (
                {"input": 1},
                {
                    "input": {
                        "value": 1,
                        "schema": {},
                        "description": None,
                    }
                },
            ),
            (None, {}),
        ],
    )
    def test_serialzed_params(
        self, input_params: ParamsDict | dict[str, Any] | None, expected_params: dict[str, Any]
    ) -> None:
        hitl_op = HITLOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            options=["1", "2", "3", "4", "5"],
            params=input_params,
        )
        if AIRFLOW_V_3_2_PLUS:
            for key in expected_params:
                expected_params[key]["source"] = "task"

        assert hitl_op.serialized_params == expected_params

    @pytest.mark.skipif(
        AIRFLOW_V_3_1_3_PLUS,
        reason="Preserve the old behavior if airflow-core < 3.1.3. Otherwise the UI will break.",
    )
    def test_serialzed_params_legacy(self) -> None:
        hitl_op = HITLOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            options=["1", "2", "3", "4", "5"],
            params={"input": Param(1)},
        )
        assert hitl_op.serialized_params == {"input": 1}

    def test_execute_complete(self) -> None:
        hitl_op = HITLOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            options=["1", "2", "3", "4", "5"],
            params={"input": 1},
        )

        responded_at_dt = timezone.utcnow()

        ret = hitl_op.execute_complete(
            context={},
            event={
                "chosen_options": ["1"],
                "params_input": {"input": 2},
                "responded_at": responded_at_dt,
                "responded_by_user": {"id": "test", "name": "test"},
            },
        )

        assert ret == {
            "chosen_options": ["1"],
            "params_input": {"input": 2},
            "responded_at": responded_at_dt,
            "responded_by_user": {"id": "test", "name": "test"},
        }

    @pytest.mark.parametrize(
        ("event", "expected_exception"),
        [
            ({"error": "unknown", "error_type": "unknown"}, HITLTriggerEventError),
            ({"error": "this is timeotu", "error_type": "timeout"}, HITLTimeoutError),
        ],
    )
    def test_process_trigger_event_error(
        self,
        event: dict[str, Any],
        expected_exception: type[Exception],
    ) -> None:
        hitl_op = HITLOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            options=["1", "2", "3", "4", "5"],
            params={"input": 1},
        )
        with pytest.raises(expected_exception):
            hitl_op.process_trigger_event_error(event)

    def test_validate_chosen_options_with_invalid_content(self) -> None:
        hitl_op = HITLOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            options=["1", "2", "3", "4", "5"],
            params={"input": 1},
        )

        with pytest.raises(ValueError, match="not exists"):
            hitl_op.execute_complete(
                context={},
                event={
                    "chosen_options": ["not exists"],
                    "params_input": {"input": 2},
                    "responded_by_user": {"id": "test", "name": "test"},
                },
            )

    @pytest.mark.parametrize(
        ("params", "params_input", "exc", "error_msg"),
        (
            (
                ParamsDict({"input": 1}),
                {"no such key": 2, "input": 333},
                ValueError,
                "params_input {'no such key': 2, 'input': 333} does not match params {'input': 1}",
            ),
            (
                ParamsDict({"input": Param(3, type="number", minimum=3)}),
                {"input": 0},
                ParamValidationError,
                (
                    "Invalid input for param input: 0 is less than the minimum of 3\n\n"
                    "Failed validating 'minimum' in schema:\n.*"
                ),
            ),
        ),
    )
    def test_validate_params_input_with_invalid_input(
        self,
        params: ParamsDict,
        params_input: dict[str, Any],
        exc: type[ValueError | ParamValidationError],
        error_msg: str,
    ) -> None:
        hitl_op = HITLOperator(
            task_id="hitl_test",
            subject="This is subject",
            body="This is body",
            options=["1", "2", "3", "4", "5"],
            params=params,
        )

        with pytest.raises(exc, match=error_msg):
            hitl_op.execute_complete(
                context={},
                event={
                    "chosen_options": ["1"],
                    "params_input": params_input,
                    "responded_by_user": {"id": "test", "name": "test"},
                },
            )

    @pytest.mark.parametrize(
        ("options", "params_input", "expected_parsed_query"),
        [
            (None, None, {"map_index": ["-1"]}),
            ("1", None, {"_options": ["['1']"], "map_index": ["-1"]}),
            (["1", "2"], None, {"_options": ["['1', '2']"], "map_index": ["-1"]}),
            (None, {"input_1": "123"}, {"input_1": ["123"], "map_index": ["-1"]}),
            (
                ["3", "4", "5"],
                {"input_1": "123123", "input_2": "345345"},
                {
                    "_options": ["['3', '4', '5']"],
                    "input_1": ["123123"],
                    "input_2": ["345345"],
                    "map_index": ["-1"],
                },
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
    @pytest.mark.parametrize(
        "conf_base_url",
        [None, "http://localhost:8080/"],
        ids=["no_conf_url", "with_conf_url"],
    )
    @pytest.mark.parametrize(
        "base_url",
        ["http://test", "http://test_2:8080"],
        ids=["url_1", "url_2"],
    )
    def test_generate_link_to_ui(
        self,
        base_url: str,
        conf_base_url: str,
        options: list[str] | None,
        params_input: dict[str, Any] | None,
        expected_parsed_query: dict[str, list[str]],
        hitl_task_and_ti_for_generating_link: tuple[HITLOperator, TaskInstance],
    ) -> None:
        with conf_vars({("api", "base_url"): conf_base_url}):
            if conf_base_url:
                base_url = conf_base_url
            task, ti = hitl_task_and_ti_for_generating_link
            url = task.generate_link_to_ui(
                task_instance=ti,
                base_url=base_url,
                options=options,
                params_input=params_input,
            )

            base_url_parsed_result = urlparse(base_url)
            parse_result = urlparse(url)
            assert parse_result.scheme == base_url_parsed_result.scheme
            assert parse_result.netloc == base_url_parsed_result.netloc
            assert parse_result.path == "/dags/test_dag/runs/test/tasks/hitl_test/required_actions"
            assert parse_result.params == ""
            assert parse_qs(parse_result.query) == expected_parsed_query

    @pytest.mark.parametrize(
        ("options", "params_input", "expected_err_msg"),
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
        options: list[Any] | None,
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
        with pytest.raises(ValueError, match="Passing options to ApprovalOperator is not allowed."):
            ApprovalOperator(
                task_id="hitl_test",
                subject="This is subject",
                body="This is body",
                options=["1", "2", "3", "4", "5"],
                params={"input": 1},
            )

    def test_init_with_multiple_set_to_true(self) -> None:
        with pytest.raises(ValueError, match="Passing multiple to ApprovalOperator is not allowed."):
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

        responded_at_dt = timezone.utcnow()

        ret = hitl_op.execute_complete(
            context={},
            event={
                "chosen_options": ["Approve"],
                "params_input": {},
                "responded_at": responded_at_dt,
                "responded_by_user": {"id": "test", "name": "test"},
            },
        )

        assert ret == {
            "chosen_options": ["Approve"],
            "params_input": {},
            "responded_at": responded_at_dt,
            "responded_by_user": {"id": "test", "name": "test"},
        }

    def test_execute_complete_with_downstream_tasks(
        self, dag_maker: DagMaker, get_context_from_model_ti: Any
    ) -> None:
        with dag_maker("hitl_test_dag", serialized=True):
            hitl_op = ApprovalOperator(
                task_id="hitl_test",
                subject="This is subject",
            )
            hitl_op >> EmptyOperator(task_id="op1")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("hitl_test")
        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            hitl_op.execute_complete(
                context=get_context_from_model_ti(ti, hitl_op),
                event={
                    "chosen_options": ["Reject"],
                    "params_input": {},
                    "responded_at": timezone.utcnow(),
                    "responded_by_user": {"id": "test", "name": "test"},
                },
            )
        assert set(exc_info.value.tasks) == {"op1"}

    def test_execute_complete_with_fail_on_reject_set_to_true(
        self, dag_maker: DagMaker, get_context_from_model_ti: Any
    ) -> None:
        with dag_maker("hitl_test_dag", serialized=True):
            hitl_op = ApprovalOperator(task_id="hitl_test", subject="This is subject", fail_on_reject=True)
            hitl_op >> EmptyOperator(task_id="op1")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("hitl_test")
        with pytest.raises(HITLRejectException):
            hitl_op.execute_complete(
                context=get_context_from_model_ti(ti, hitl_op),
                event={
                    "chosen_options": ["Reject"],
                    "params_input": {},
                    "responded_at": timezone.utcnow(),
                    "responded_by_user": {"id": "test", "name": "test"},
                },
            )


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
    def test_execute_complete(self, dag_maker: DagMaker, get_context_from_model_ti: Any) -> None:
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
                context=get_context_from_model_ti(ti, branch_op),
                event={
                    "chosen_options": ["branch_1"],
                    "params_input": {},
                    "responded_at": timezone.utcnow(),
                    "responded_by_user": {"id": "test", "name": "test"},
                },
            )
        assert set(exc_info.value.tasks) == set((f"branch_{i}", -1) for i in range(2, 6))

    def test_execute_complete_with_multiple_branches(
        self, dag_maker: DagMaker, get_context_from_model_ti: Any
    ) -> None:
        with dag_maker("hitl_test_dag", serialized=True):
            branch_op = HITLBranchOperator(
                task_id="make_choice",
                subject="This is subject",
                multiple=True,
                options=[f"branch_{i}" for i in range(1, 6)],
            )

            branch_op >> [EmptyOperator(task_id=f"branch_{i}") for i in range(1, 6)]

        responded_at_dt = timezone.utcnow()

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("make_choice")
        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            branch_op.execute_complete(
                context=get_context_from_model_ti(ti, branch_op),
                event={
                    "chosen_options": [f"branch_{i}" for i in range(1, 4)],
                    "params_input": {},
                    "responded_at": responded_at_dt,
                    "responded_by_user": {"id": "test", "name": "test"},
                },
            )
        assert set(exc_info.value.tasks) == set((f"branch_{i}", -1) for i in range(4, 6))

    def test_mapping_applies_for_single_choice(
        self, dag_maker: DagMaker, get_context_from_model_ti: Any
    ) -> None:
        # ["Approve"]; map -> "publish"
        with dag_maker("hitl_map_dag", serialized=True):
            op = HITLBranchOperator(
                task_id="choose",
                subject="S",
                options=["Approve", "Reject"],
                options_mapping={"Approve": "publish"},
            )
            op >> [EmptyOperator(task_id="publish"), EmptyOperator(task_id="archive")]

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("choose")

        with pytest.raises(DownstreamTasksSkipped) as exc:
            op.execute_complete(
                context=get_context_from_model_ti(ti, op),
                event={
                    "chosen_options": ["Approve"],
                    "params_input": {},
                    "responded_at": timezone.utcnow(),
                    "responded_by_user": {"id": "test", "name": "test"},
                },
            )
        # checks to see that the "archive" task was skipped
        assert set(exc.value.tasks) == {("archive", -1)}

    def test_mapping_with_multiple_choices(self, dag_maker: DagMaker, get_context_from_model_ti: Any) -> None:
        # multiple=True; mapping applied per option; no dedup implied
        with dag_maker("hitl_map_dag", serialized=True):
            op = HITLBranchOperator(
                task_id="choose",
                subject="S",
                multiple=True,
                options=["Approve", "KeepAsIs"],
                options_mapping={"Approve": "publish", "KeepAsIs": "keep"},
            )
            op >> [
                EmptyOperator(task_id="publish"),
                EmptyOperator(task_id="keep"),
                EmptyOperator(task_id="other"),
            ]

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("choose")

        with pytest.raises(DownstreamTasksSkipped) as exc:
            op.execute_complete(
                context=get_context_from_model_ti(ti, op),
                event={
                    "chosen_options": ["Approve", "KeepAsIs"],
                    "params_input": {},
                    "responded_at": timezone.utcnow(),
                    "responded_by_user": {"id": "test", "name": "test"},
                },
            )
        # publish + keep chosen → only "other" skipped
        assert set(exc.value.tasks) == {("other", -1)}

    def test_fallback_to_option_when_not_mapped(
        self, dag_maker: DagMaker, get_context_from_model_ti: Any
    ) -> None:
        # No mapping: option must match downstream task_id
        with dag_maker("hitl_map_dag", serialized=True):
            op = HITLBranchOperator(
                task_id="choose",
                subject="S",
                options=["branch_1", "branch_2"],  # no mapping for branch_2
            )
            op >> [EmptyOperator(task_id="branch_1"), EmptyOperator(task_id="branch_2")]

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("choose")

        with pytest.raises(DownstreamTasksSkipped) as exc:
            op.execute_complete(
                context=get_context_from_model_ti(ti, op),
                event={
                    "chosen_options": ["branch_2"],
                    "params_input": {},
                    "responded_at": timezone.utcnow(),
                    "responded_by_user": {"id": "test", "name": "test"},
                },
            )
        assert set(exc.value.tasks) == {("branch_1", -1)}

    def test_error_if_mapped_branch_not_direct_downstream(
        self, dag_maker: DagMaker, get_context_from_model_ti: Any
    ) -> None:
        # Don't add the mapped task downstream → expect a clean error
        with dag_maker("hitl_map_dag", serialized=True):
            op = HITLBranchOperator(
                task_id="choose",
                subject="S",
                options=["Approve"],
                options_mapping={"Approve": "not_a_downstream"},
            )
            # Intentionally no downstream "not_a_downstream"

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("choose")

        with pytest.raises(AirflowException, match="downstream|not found"):
            op.execute_complete(
                context=get_context_from_model_ti(ti, op),
                event={
                    "chosen_options": ["Approve"],
                    "params_input": {},
                    "responded_at": timezone.utcnow(),
                    "responded_by_user": {"id": "test", "name": "test"},
                },
            )

    @pytest.mark.parametrize("bad", [123, ["publish"], {"x": "y"}, b"publish"])
    def test_options_mapping_non_string_value_raises(self, bad: Any) -> None:
        with pytest.raises(ValueError, match=r"values must be strings \(task_ids\)"):
            HITLBranchOperator(
                task_id="choose",
                subject="S",
                options=["Approve"],
                options_mapping={"Approve": bad},
            )

    def test_options_mapping_key_not_in_options_raises(self) -> None:
        with pytest.raises(ValueError, match="contains keys that are not in `options`"):
            HITLBranchOperator(
                task_id="choose",
                subject="S",
                options=["Approve", "Reject"],
                options_mapping={"NotAnOption": "publish"},
            )


class TestHITLSummaryForListeners:
    """Verify hitl_summary dict at all lifecycle stages: __init__, execute, execute_complete."""

    def test_hitl_operator_init_all_fields(self) -> None:
        """hitl_summary is exactly the expected dict after __init__ with all fields set."""
        op = HITLOperator(
            task_id="test",
            subject="Please review",
            body="Details here",
            options=["Yes", "No"],
            defaults=["Yes"],
            multiple=False,
            assigned_users=HITLUser(id="u1", name="Alice"),
            params={"env": Param("prod", type="string", description="Target env")},
        )

        assert op.hitl_summary == {
            "subject": "Please review",
            "body": "Details here",
            "options": ["Yes", "No"],
            "defaults": ["Yes"],
            "multiple": False,
            "assigned_users": [{"id": "u1", "name": "Alice"}],
            "serialized_params": op.serialized_params,
        }

    def test_hitl_operator_init_minimal(self) -> None:
        """hitl_summary with only required fields; optional ones default to None."""
        op = HITLOperator(
            task_id="test",
            subject="Review",
            options=["A", "B"],
        )

        assert op.hitl_summary == {
            "subject": "Review",
            "body": None,
            "options": ["A", "B"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
        }

    def test_approval_operator_init_summary(self) -> None:
        """ApprovalOperator hitl_summary includes base + approval-specific fields."""
        op = ApprovalOperator(
            task_id="test",
            subject="Deploy?",
            ignore_downstream_trigger_rules=True,
            fail_on_reject=True,
        )

        assert op.hitl_summary == {
            "subject": "Deploy?",
            "body": None,
            "options": ["Approve", "Reject"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
            "ignore_downstream_trigger_rules": True,
            "fail_on_reject": True,
        }

    def test_approval_operator_init_defaults(self) -> None:
        """ApprovalOperator with default settings."""
        op = ApprovalOperator(task_id="test", subject="Deploy?")

        assert op.hitl_summary == {
            "subject": "Deploy?",
            "body": None,
            "options": ["Approve", "Reject"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
            "ignore_downstream_trigger_rules": False,
            "fail_on_reject": False,
        }

    def test_hitl_branch_operator_init_with_mapping(self) -> None:
        """HITLBranchOperator hitl_summary includes base + options_mapping."""
        op = HITLBranchOperator(
            task_id="test",
            subject="Choose",
            options=["A", "B"],
            options_mapping={"A": "task_a", "B": "task_b"},
        )

        assert op.hitl_summary == {
            "subject": "Choose",
            "body": None,
            "options": ["A", "B"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
            "options_mapping": {"A": "task_a", "B": "task_b"},
        }

    def test_hitl_branch_operator_init_without_mapping(self) -> None:
        """HITLBranchOperator stores empty dict for options_mapping when not provided."""
        op = HITLBranchOperator(
            task_id="test",
            subject="Choose",
            options=["A", "B"],
        )

        assert op.hitl_summary == {
            "subject": "Choose",
            "body": None,
            "options": ["A", "B"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
            "options_mapping": {},
        }

    def test_hitl_entry_operator_init_summary(self) -> None:
        """HITLEntryOperator hitl_summary includes base fields with OK defaults."""
        op = HITLEntryOperator(
            task_id="test",
            subject="Enter data",
            params={"name": Param("", type="string")},
        )

        assert op.hitl_summary == {
            "subject": "Enter data",
            "body": None,
            "options": ["OK"],
            "defaults": ["OK"],
            "multiple": False,
            "assigned_users": None,
            "serialized_params": op.serialized_params,
        }

    def test_hitl_entry_operator_init_custom_options(self) -> None:
        """HITLEntryOperator with explicit options and no defaults."""
        op = HITLEntryOperator(
            task_id="test",
            subject="Confirm",
            options=["OK", "Cancel"],
        )

        assert op.hitl_summary == {
            "subject": "Confirm",
            "body": None,
            "options": ["OK", "Cancel"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
        }

    def test_execute_enriches_summary_with_timeout(self) -> None:
        """execute() adds timeout_datetime; all other init keys remain."""
        op = HITLOperator(
            task_id="test",
            subject="Review",
            options=["OK"],
            execution_timeout=datetime.timedelta(minutes=10),
        )

        with (
            patch("airflow.providers.standard.operators.hitl.upsert_hitl_detail"),
            patch.object(op, "defer"),
        ):
            op.execute({"task_instance": MagicMock(id=uuid4())})  # type: ignore[arg-type]

        s = op.hitl_summary
        # Validate the timeout value is a parseable ISO string
        timeout_dt = datetime.datetime.fromisoformat(s["timeout_datetime"])
        assert timeout_dt is not None

        assert s == {
            "subject": "Review",
            "body": None,
            "options": ["OK"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
            "timeout_datetime": s["timeout_datetime"],
        }

    def test_execute_without_timeout(self) -> None:
        """execute() sets timeout_datetime to None when no execution_timeout."""
        op = HITLOperator(
            task_id="test",
            subject="Review",
            options=["OK"],
        )

        with (
            patch("airflow.providers.standard.operators.hitl.upsert_hitl_detail"),
            patch.object(op, "defer"),
        ):
            op.execute({"task_instance": MagicMock(id=uuid4())})  # type: ignore[arg-type]

        assert op.hitl_summary == {
            "subject": "Review",
            "body": None,
            "options": ["OK"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
            "timeout_datetime": None,
        }

    def test_hitl_operator_execute_complete_enriches_summary(self) -> None:
        """execute_complete() adds response fields directly into hitl_summary."""
        op = HITLOperator(
            task_id="test",
            subject="Review",
            options=["1", "2"],
            params={"input": 1},
        )

        responded_at = timezone.utcnow()
        op.execute_complete(
            context={},
            event={
                "chosen_options": ["1"],
                "params_input": {"input": 1},
                "responded_at": responded_at,
                "responded_by_user": {"id": "u1", "name": "Alice"},
            },
        )

        assert op.hitl_summary == {
            "subject": "Review",
            "body": None,
            "options": ["1", "2"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": op.serialized_params,
            "chosen_options": ["1"],
            "params_input": {"input": 1},
            "responded_at": responded_at.isoformat(),
            "responded_by_user": {"id": "u1", "name": "Alice"},
        }

    def test_hitl_operator_execute_complete_error_stores_error_type(self) -> None:
        """execute_complete() stores error_type in hitl_summary on error events."""
        op = HITLOperator(
            task_id="test",
            subject="Review",
            options=["OK"],
        )

        with pytest.raises(HITLTimeoutError):
            op.execute_complete(
                context={},
                event={"error": "timed out", "error_type": "timeout"},
            )

        assert op.hitl_summary == {
            "subject": "Review",
            "body": None,
            "options": ["OK"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
            "error_type": "timeout",
        }

    def test_approval_operator_execute_complete_approved(self) -> None:
        """Approving sets approved=True in hitl_summary."""
        op = ApprovalOperator(task_id="test", subject="Deploy?")

        responded_at = timezone.utcnow()
        op.execute_complete(
            context={},
            event={
                "chosen_options": ["Approve"],
                "params_input": {},
                "responded_at": responded_at,
                "responded_by_user": {"id": "u1", "name": "Alice"},
            },
        )

        assert op.hitl_summary == {
            "subject": "Deploy?",
            "body": None,
            "options": ["Approve", "Reject"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
            "ignore_downstream_trigger_rules": False,
            "fail_on_reject": False,
            "chosen_options": ["Approve"],
            "params_input": {},
            "responded_at": responded_at.isoformat(),
            "responded_by_user": {"id": "u1", "name": "Alice"},
            "approved": True,
        }

    def test_approval_operator_execute_complete_rejected(self) -> None:
        """Rejecting sets approved=False in hitl_summary."""
        op = ApprovalOperator(task_id="test", subject="Deploy?")

        responded_at = timezone.utcnow()
        op.execute_complete(
            context={},
            event={
                "chosen_options": ["Reject"],
                "params_input": {},
                "responded_at": responded_at,
                "responded_by_user": {"id": "u1", "name": "Alice"},
            },
        )

        assert op.hitl_summary == {
            "subject": "Deploy?",
            "body": None,
            "options": ["Approve", "Reject"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
            "ignore_downstream_trigger_rules": False,
            "fail_on_reject": False,
            "chosen_options": ["Reject"],
            "params_input": {},
            "responded_at": responded_at.isoformat(),
            "responded_by_user": {"id": "u1", "name": "Alice"},
            "approved": False,
        }

    def test_hitl_branch_operator_execute_complete_records_branches(
        self, dag_maker: DagMaker, get_context_from_model_ti: Any
    ) -> None:
        """HITLBranchOperator stores branches_to_execute in hitl_summary."""
        with dag_maker("hitl_summary_dag", serialized=True):
            op = HITLBranchOperator(
                task_id="choose",
                subject="Choose",
                options=["A", "B"],
                options_mapping={"A": "task_a", "B": "task_b"},
            )
            op >> [EmptyOperator(task_id="task_a"), EmptyOperator(task_id="task_b")]

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("choose")

        responded_at = timezone.utcnow()
        with pytest.raises(DownstreamTasksSkipped):
            op.execute_complete(
                context=get_context_from_model_ti(ti, op),
                event={
                    "chosen_options": ["A"],
                    "params_input": {},
                    "responded_at": responded_at,
                    "responded_by_user": {"id": "u1", "name": "Alice"},
                },
            )

        assert op.hitl_summary == {
            "subject": "Choose",
            "body": None,
            "options": ["A", "B"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
            "options_mapping": {"A": "task_a", "B": "task_b"},
            "chosen_options": ["A"],
            "params_input": {},
            "responded_at": responded_at.isoformat(),
            "responded_by_user": {"id": "u1", "name": "Alice"},
            "branches_to_execute": ["task_a"],
        }

    def test_full_lifecycle_approval(self) -> None:
        """Verify exact hitl_summary at each stage: __init__ -> execute -> execute_complete."""
        op = ApprovalOperator(
            task_id="test",
            subject="Release v2.0?",
            body="Please approve the production deployment.",
            execution_timeout=datetime.timedelta(minutes=30),
        )

        # -- After __init__: only base + approval keys --
        assert op.hitl_summary == {
            "subject": "Release v2.0?",
            "body": "Please approve the production deployment.",
            "options": ["Approve", "Reject"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
            "ignore_downstream_trigger_rules": False,
            "fail_on_reject": False,
        }

        # -- After execute (mocked defer): timeout_datetime added --
        with (
            patch("airflow.providers.standard.operators.hitl.upsert_hitl_detail"),
            patch.object(op, "defer"),
        ):
            op.execute({"task_instance": MagicMock(id=uuid4())})  # type: ignore[arg-type]

        s = op.hitl_summary
        timeout_dt_str = s["timeout_datetime"]
        assert timeout_dt_str is not None
        datetime.datetime.fromisoformat(timeout_dt_str)

        assert s == {
            "subject": "Release v2.0?",
            "body": "Please approve the production deployment.",
            "options": ["Approve", "Reject"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
            "ignore_downstream_trigger_rules": False,
            "fail_on_reject": False,
            "timeout_datetime": timeout_dt_str,
        }

        # -- After execute_complete: response + approved fields added --
        responded_at = timezone.utcnow()
        op.execute_complete(
            context={},
            event={
                "chosen_options": ["Approve"],
                "params_input": {},
                "responded_at": responded_at,
                "responded_by_user": {"id": "admin", "name": "Admin"},
            },
        )

        assert s == {
            "subject": "Release v2.0?",
            "body": "Please approve the production deployment.",
            "options": ["Approve", "Reject"],
            "defaults": None,
            "multiple": False,
            "assigned_users": None,
            "serialized_params": None,
            "ignore_downstream_trigger_rules": False,
            "fail_on_reject": False,
            "timeout_datetime": timeout_dt_str,
            "chosen_options": ["Approve"],
            "params_input": {},
            "responded_at": responded_at.isoformat(),
            "responded_by_user": {"id": "admin", "name": "Admin"},
            "approved": True,
        }
