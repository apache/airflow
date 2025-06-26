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
from typing import TYPE_CHECKING, Any, Callable

from airflow.models import SkipMixin
from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.execution_time.hitl import add_input_request
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
        options: list[str],
        subject: str,
        python_callable: Callable,
        body: str | None = None,
        default: str | None = None,
        params: ParamsDict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.options = options
        self.subject = subject
        self.python_callable = python_callable
        self.body = body
        self.params = params or {}
        self.default = default

    def execute(self, context: Context):
        ti_id = context["task_instance"].id
        add_input_request(
            ti_id=ti_id,
            options=self.options,
            subject=self.subject,
            body=self.body,
            params=self.params,
            default=self.default,
        )
        self.defer(
            trigger=HITLTrigger(
                ti_id=ti_id,
                options=self.options,
                default=self.default,
            ),
            method_name="execute_complete",
        )

    @staticmethod
    def get_user_response(event: dict[str, Any]) -> str:
        return event["content"]

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        user_response = self.get_user_response(event)
        return self.python_callable(user_response)


class ApprovalOperator(HITLOperator):
    """Convenience operator for approval tasks."""

    def __init__(
        self,
        *,
        subject: str,
        python_callable: Callable,
        body: str | None = None,
        params: ParamsDict | None = None,
        default: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            options=["Approve", "Reject"],
            subject=subject,
            python_callable=python_callable,
            body=body,
            params=params,
            default=default,
            **kwargs,
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        user_response = self.get_user_response(event)
        if user_response != "Approve" and user_response != "Reject":
            # TODO: update message
            raise ValueError("")
        return super().execute_complete(context, event)


class HITLTerminationOperator(HITLOperator, SkipMixin):
    """ShortCirquitOperator to terminate the Dag run by human decision."""

    def __init__(
        self,
        *,
        subject: str,
        python_callable: Callable,
        body: str | None = None,
        params: ParamsDict | None = None,
        default: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            options=["Stop", "Proceed"],
            subject=subject,
            python_callable=python_callable,
            body=body,
            params=params,
            default=default,
            **kwargs,
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        raise NotImplementedError


class HITLBranchOperator(HITLOperator):
    """SkipMixIn to implement a branching functionality based on human selection."""

    def __init__(
        self,
        *,
        options: list[str],
        subject: str,
        python_callable: Callable,
        body: str | None = None,
        params: ParamsDict | None = None,
        default: str | None = None,
        multiple: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(
            options=options,
            subject=subject,
            python_callable=python_callable,
            body=body,
            params=params,
            default=default,
            **kwargs,
        )
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
        *,
        subject: str,
        python_callable: Callable,
        body: str | None = None,
        params: ParamsDict | None = None,
        options: list[str] | None = None,
        default: str | None = None,
        **kwargs,
    ) -> None:
        if options is None:
            options = ["OK"]
            default = "OK"

        super().__init__(
            options=options,
            subject=subject,
            python_callable=python_callable,
            body=body,
            params=params,
            default=default,
            **kwargs,
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        raise NotImplementedError
