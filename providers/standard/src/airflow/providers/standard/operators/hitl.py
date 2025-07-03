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

import logging
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from airflow.models import SkipMixin
from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.exceptions import HITLTriggerEventError
from airflow.providers.standard.execution_time.hitl import add_hitl_input_request
from airflow.providers.standard.triggers.hitl import HITLTrigger
from airflow.providers.standard.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.sdk.definitions.param import ParamsDict

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context

log = logging.getLogger(__name__)
if not AIRFLOW_V_3_1_PLUS:
    log.warning("Human in the loop functionality needs Airflow 3.1+..")


class HITLOperator(BaseOperator):
    """
    Base class for all Human-in-the-loop Operators to inherit from.

    :param subject: Headline/subject presented to the user for the interaction task
    :param options: List of options that the human can select from and click to complete the task.
        Buttons on the UI will be presented in the order of the list
    :param body: descriptive text that might give background, hints or can provide background or summary of
        details that are needed to decide
    :param default: The default result (highlighted button) and result that is taken if timeout is passed
    :param params: dictionary of parameter definitions that are in the format of Dag params such that
        a Form Field can be rendered. Entered data is validated (schema, required fields) like for a Dag run
        and added to XCom of the task result
    """

    template_fields: Sequence[str] = ("subject", "body")

    allow_arbitrary_input: bool = False

    def __init__(
        self,
        *,
        subject: str,
        options: list[str],
        body: str | None = None,
        default: str | list[str] | None = None,
        params: ParamsDict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.options = options
        self.subject = subject
        self.body = body
        self.params = params or {}
        self.multiple = False
        self.default = [default] if isinstance(default, str) else default

        self.validate_default()

    def validate_default(self) -> None:
        if self.default is None and self.execution_timeout:
            raise ValueError('"default" is requied when "execution_timeout" is provided.')

    def execute(self, context: Context):
        ti_id = context["task_instance"].id
        # Write Human-in-the-loop input request to DB
        add_hitl_input_request(
            ti_id=ti_id,
            options=self.options,
            subject=self.subject,
            body=self.body,
            default=self.default,
            multiple=self.multiple,
            params=self.serializable_params,
        )
        self.log.info("Waiting for response")
        if self.execution_timeout:
            timeout_datetime = datetime.now(timezone.utc) + self.execution_timeout
        else:
            timeout_datetime = None
        # Defer the Human-in-the-loop response checking process to HITLTrigger
        self.defer(
            trigger=HITLTrigger(
                ti_id=ti_id,
                options=self.options,
                default=self.default,
                params=self.serializable_params,
                multiple=self.multiple,
                timeout_datetime=timeout_datetime,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        if "error" in event:
            raise HITLTriggerEventError(event["error"])

        response_content = event["response_content"]
        params_input = event["params_input"]
        if self.allow_arbitrary_input:
            self.validate_response_content(response_content)
        self.validate_params_input(params_input)
        return {
            "response_content": response_content,
            "params_input": params_input,
        }

    def validate_response_content(self, response_content: str | list[str]) -> None:
        if isinstance(response_content, list):
            if self.multiple is False:
                raise ValueError(
                    f"Multiple response {response_content} received while multiple is set to False"
                )

            if diff := set(response_content) - set(self.options):
                raise ValueError(f"Responses {diff} not in {self.options}")

        if response_content not in self.options:
            raise ValueError(f"Response {response_content} not in {self.options}")

    def validate_params_input(self, params_input: dict | None) -> None:
        if (
            self.serializable_params is not None
            and params_input is not None
            and set(self.serializable_params.keys()) ^ set(params_input)
        ):
            raise ValueError(f"params_input {params_input} does not match params {self.params}")

    @property
    def serializable_params(self) -> dict[str, Any] | None:
        return self.params.dump() if isinstance(self.params, ParamsDict) else self.params


class ApprovalOperator(HITLOperator):
    """Human-in-the-loop Operator that has only 'Approval' and 'Reject' options."""

    def __init__(self, **kwargs) -> None:
        if "options" in kwargs:
            kwargs.pop("options")
            self.log.warning("Passing options into ApprovalOperator will be ignored.")
        super().__init__(options=["Approve", "Reject"], **kwargs)


class HITLTerminationOperator(HITLOperator, SkipMixin):
    """ShortCirquitOperator to terminate the Dag run by human decision."""

    def __init__(self, **kwargs) -> None:
        if "options" in kwargs:
            kwargs.pop("options")
            self.log.warning("Passing options into ApprovalOperator will be ignored.")
        super().__init__(options=["Stop", "Proceed"], **kwargs)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        raise NotImplementedError


class HITLBranchOperator(HITLOperator):
    """BranchOperator based on Human-in-the-loop Response."""

    def __init__(self, *, multiple: bool = False, **kwargs) -> None:
        super().__init__(**kwargs)
        self.multiple = multiple

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        raise NotImplementedError


class HITLEntryOperator(HITLOperator):
    """
    User can add further information with all options that a TriggerForm allows (same like Dag params).

    Options and default default to ["OK"] but can be over-ridden.
    """

    def __init__(
        self,
        **kwargs,
    ) -> None:
        if "options" not in kwargs:
            kwargs["options"] = ["OK"]
            kwargs["default"] = ["OK"]

        super().__init__(**kwargs)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        raise NotImplementedError
