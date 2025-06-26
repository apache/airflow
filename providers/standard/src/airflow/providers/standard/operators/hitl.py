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

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from airflow.models import SkipMixin
from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.execution_time.hitl import add_hitl_input_request
from airflow.providers.standard.triggers.hitl import HITLTrigger

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.param import ParamsDict


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

    def __init__(
        self,
        *,
        subject: str,
        options: list[str],
        body: str | None = None,
        default: str | None = None,
        params: ParamsDict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.options = options
        self.subject = subject
        self.body = body
        self.params = params or {}
        self.default = default

        self.multiple = False

    def execute(self, context: Context):
        ti_id = context["task_instance"].id
        # Write Human-in-the-loop input request to DB
        add_hitl_input_request(
            ti_id=ti_id,
            options=self.options,
            subject=self.subject,
            body=self.body,
            params=self.params,
            default=self.default,
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
                timeout_datetime=timeout_datetime,
                multiple=self.multiple,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> object:
        return event["content"]


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
