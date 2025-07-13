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

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.standard.version_compat import AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    raise AirflowOptionalProviderFeatureException("Human in the loop functionality needs Airflow 3.1+.")


from collections.abc import Collection, Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.exceptions import HITLTimeoutError, HITLTriggerEventError
from airflow.providers.standard.triggers.hitl import HITLTrigger, HITLTriggerEventSuccessPayload
from airflow.providers.standard.utils.skipmixin import SkipMixin
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
    :param defaults: The default options and the options that are taken if timeout is passed.
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
        defaults: str | list[str] | None = None,
        multiple: bool = False,
        params: ParamsDict | dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.subject = subject
        self.body = body

        self.options = options
        # allow defaults to store more than one options when multiple=True
        self.defaults = [defaults] if isinstance(defaults, str) else defaults
        self.multiple = multiple

        self.params: ParamsDict = params if isinstance(params, ParamsDict) else ParamsDict(params or {})

        self.validate_defaults()

    def validate_defaults(self) -> None:
        """
        Validate whether the given defaults pass the following criteria.

        1. Default options should be the subset of options.
        2. When multiple is False, there should only be one option.
        """
        if self.defaults is not None:
            if not set(self.defaults).issubset(self.options):
                raise ValueError(f'defaults "{self.defaults}" should be a subset of options "{self.options}"')

            if self.multiple is False and len(self.defaults) > 1:
                raise ValueError('More than one defaults given when "multiple" is set to False.')

    def execute(self, context: Context):
        """Add a Human-in-the-loop Response and then defer to HITLTrigger and wait for user input."""
        ti_id = context["task_instance"].id
        # Write Human-in-the-loop input request to DB
        add_hitl_detail(
            ti_id=ti_id,
            options=self.options,
            subject=self.subject,
            body=self.body,
            defaults=self.defaults,
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
                defaults=self.defaults,
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
            self.process_trigger_event_error(event)

        chosen_options = event["chosen_options"]
        params_input = event["params_input"] or {}
        self.validate_chosen_options(chosen_options)
        self.validate_params_input(params_input)
        return HITLTriggerEventSuccessPayload(
            chosen_options=chosen_options,
            params_input=params_input,
        )

    def process_trigger_event_error(self, event: dict[str, Any]) -> None:
        if "error_type" == "timeout":
            raise HITLTimeoutError(event)

        raise HITLTriggerEventError(event)

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


class ApprovalOperator(HITLOperator, SkipMixin):
    """Human-in-the-loop Operator that has only 'Approval' and 'Reject' options."""

    inherits_from_skipmixin = True

    FIXED_ARGS = ["options", "multiple"]

    def __init__(self, ignore_downstream_trigger_rules: bool = False, **kwargs) -> None:
        for arg in self.FIXED_ARGS:
            if arg in kwargs:
                raise ValueError(f"Passing {arg} to ApprovalOperator is not allowed.")

        self.ignore_downstream_trigger_rules = ignore_downstream_trigger_rules

        super().__init__(options=["Approve", "Reject"], multiple=False, **kwargs)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        ret = super().execute_complete(context=context, event=event)

        chosen_option = ret["chosen_options"][0]
        if chosen_option == "Approve":
            self.log.info("Approved. Proceeding with downstream tasks...")
            return ret

        if not self.downstream_task_ids:
            self.log.info("No downstream tasks; nothing to do.")
            return ret

        def get_tasks_to_skip():
            if self.ignore_downstream_trigger_rules is True:
                tasks = context["task"].get_flat_relatives(upstream=False)
            else:
                tasks = context["task"].get_direct_relatives(upstream=False)

            yield from (t for t in tasks if not t.is_teardown)

        tasks_to_skip = get_tasks_to_skip()

        # this lets us avoid an intermediate list unless debug logging
        if self.log.getEffectiveLevel() <= logging.DEBUG:
            self.log.debug("Downstream task IDs %s", tasks_to_skip := list(get_tasks_to_skip()))

        self.log.info("Skipping downstream tasks")
        self.skip(ti=context["ti"], tasks=tasks_to_skip)

        return ret


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
            kwargs["defaults"] = ["OK"]

        super().__init__(**kwargs)
