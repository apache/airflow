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

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.standard.version_compat import AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    raise AirflowOptionalProviderFeatureException("Human in the loop functionality needs Airflow 3.1+.")


from collections.abc import Collection, Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from airflow.models import SkipMixin
from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.exceptions import HITLTriggerEventError
from airflow.providers.standard.triggers.hitl import HITLTrigger, HITLTriggerEventSuccessPayload
from airflow.sdk.definitions.param import ParamsDict
from airflow.sdk.execution_time.hitl import add_hitl_detail

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context


class HITLOperator(BaseOperator):
    """
    Base class for all Human-in-the-loop Operators to inherit from.

    :param subject: Headline/subject presented to the user for the interaction task.
    :param options: List of options that the an user can select from to complete the task.
    :param body: Descriptive text (with Markdown support) that gives the details that are needed to decide.
    :param default: The default option and the option that is taken if timeout is passed.
    :param multiple: Whether the user can select one or multiple options.
    :param params: dictionary of parameter definitions that are in the format of Dag params such that
        a Form Field can be rendered. Entered data is validated (schema, required fields) like for a Dag run
        and added to XCom of the task result.
    """

    template_fields: Collection[str] = ("subject", "body")

    def __init__(
        self,
        *,
        subject: str,
        options: list[str],
        body: str | None = None,
        default: str | list[str] | None = None,
        multiple: bool = False,
        params: ParamsDict | dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.subject = subject
        self.body = body

        self.options = options
        # allow defaults to store more than one options when multiple=True
        self.default = [default] if isinstance(default, str) else default
        self.multiple = multiple

        self.params: ParamsDict = params if isinstance(params, ParamsDict) else ParamsDict(params or {})

        self.validate_defaults()

    def validate_defaults(self) -> None:
        """
        Validate whether the given default pass the following criteria.

        1. When timeout is set, default options should be provided.
        2. Default options should be the subset of options.
        3. When multiple is False, there should only be one option.
        """
        if self.default is None and self.execution_timeout:
            raise ValueError('"default" is required when "execution_timeout" is provided.')

        if self.default is not None:
            if not set(self.default).issubset(self.options):
                raise ValueError(f'default "{self.default}" should be a subset of options "{self.options}"')

            if self.multiple is False and len(self.default) > 1:
                raise ValueError('More than one default given when "multiple" is set to False.')

    def execute(self, context: Context):
        """Add a Human-in-the-loop Response and then defer to HITLTrigger and wait for user input."""
        ti_id = context["task_instance"].id
        # Write Human-in-the-loop input request to DB
        add_hitl_detail(
            ti_id=ti_id,
            options=self.options,
            subject=self.subject,
            body=self.body,
            default=self.default,
            multiple=self.multiple,
            params=self.serialzed_params,
        )
        if self.execution_timeout:
            timeout_datetime = datetime.now(timezone.utc) + self.execution_timeout
        else:
            timeout_datetime = None
        self.log.info("Waiting for response")
        # Defer the Human-in-the-loop response checking process to HITLTrigger
        self.defer(
            trigger=HITLTrigger(
                ti_id=ti_id,
                options=self.options,
                default=self.default,
                params=self.serialzed_params,
                multiple=self.multiple,
                timeout_datetime=timeout_datetime,
            ),
            method_name="execute_complete",
        )

    @property
    def serialzed_params(self) -> dict[str, Any]:
        return self.params.dump() if isinstance(self.params, ParamsDict) else self.params

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        if "error" in event:
            raise HITLTriggerEventError(event["error"])

        chosen_options = event["chosen_options"]
        params_input = event["params_input"] or {}
        self.validate_chosen_options(chosen_options)
        self.validate_params_input(params_input)
        return HITLTriggerEventSuccessPayload(
            chosen_options=chosen_options,
            params_input=params_input,
        )

    def validate_chosen_options(self, chosen_options: list[str]) -> None:
        """Check whether user provide valid response."""
        if diff := set(chosen_options) - set(self.options):
            raise ValueError(f"Responses {diff} not in {self.options}")

    def validate_params_input(self, params_input: Mapping) -> None:
        """Check whether user provide valid params input."""
        if (
            self.serialzed_params is not None
            and params_input is not None
            and set(self.serialzed_params.keys()) ^ set(params_input)
        ):
            raise ValueError(f"params_input {params_input} does not match params {self.params}")


class ApprovalOperator(HITLOperator):
    """Human-in-the-loop Operator that has only 'Approval' and 'Reject' options."""

    def __init__(self, **kwargs) -> None:
        if "options" in kwargs:
            raise ValueError("Passing options to ApprovalOperator is not allowed.")
        super().__init__(options=["Approve", "Reject"], **kwargs)


class HITLTerminationOperator(HITLOperator, SkipMixin):
    """
    Human-in-the-loop Operator that has only 'Stop' and 'Proceed' options.

    When 'Stop' is selected by user, the dag run terminates like ShortCirquitOperator.
    """

    def __init__(self, **kwargs) -> None:
        if "options" in kwargs:
            raise ValueError("Passing options to HITLTerminationOperator is not allowed.")
        super().__init__(options=["Stop", "Proceed"], **kwargs)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        raise NotImplementedError


class HITLBranchOperator(HITLOperator):
    """BranchOperator based on Human-in-the-loop Response."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        raise NotImplementedError


class HITLEntryOperator(HITLOperator):
    """Human-in-the-loop Operator that is used to accept user input through TriggerForm."""

    def __init__(self, **kwargs) -> None:
        if "options" not in kwargs:
            kwargs["options"] = ["OK"]
            kwargs["default"] = ["OK"]

        super().__init__(**kwargs)
