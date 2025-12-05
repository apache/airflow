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
from airflow.providers.standard.version_compat import AIRFLOW_V_3_1_3_PLUS, AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    raise AirflowOptionalProviderFeatureException("Human in the loop functionality needs Airflow 3.1+.")

from collections.abc import Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any
from urllib.parse import ParseResult, urlencode, urlparse, urlunparse

from airflow.configuration import conf
from airflow.providers.standard.exceptions import HITLRejectException, HITLTimeoutError, HITLTriggerEventError
from airflow.providers.standard.operators.branch import BranchMixIn
from airflow.providers.standard.triggers.hitl import HITLTrigger, HITLTriggerEventSuccessPayload
from airflow.providers.standard.utils.skipmixin import SkipMixin
from airflow.providers.standard.version_compat import BaseOperator
from airflow.sdk.bases.notifier import BaseNotifier
from airflow.sdk.definitions.param import ParamsDict
from airflow.sdk.execution_time.hitl import upsert_hitl_detail
from airflow.sdk.timezone import utcnow

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context
    from airflow.sdk.execution_time.hitl import HITLUser
    from airflow.sdk.types import RuntimeTaskInstanceProtocol


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
        notifiers: Sequence[BaseNotifier] | BaseNotifier | None = None,
        assigned_users: HITLUser | list[HITLUser] | None = None,
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
        if hasattr(ParamsDict, "filter_params_by_source"):
            # Params that exist only in Dag level does not make sense to appear in HITLOperator
            self.params = ParamsDict.filter_params_by_source(self.params, source="task")
        elif self.params:
            self.log.debug(
                "ParamsDict.filter_params_by_source not available; HITLOperator will also include Dag level params."
            )

        self.notifiers: Sequence[BaseNotifier] = (
            [notifiers] if isinstance(notifiers, BaseNotifier) else notifiers or []
        )
        self.assigned_users = [assigned_users] if isinstance(assigned_users, dict) else assigned_users

        self.validate_options()
        self.validate_params()
        self.validate_defaults()

    def validate_options(self) -> None:
        """
        Validate the `options` attribute of the instance.

        Raises:
            ValueError: If `options` is empty.
        """
        if not self.options:
            raise ValueError('"options" cannot be empty.')

    def validate_params(self) -> None:
        """
        Validate the `params` attribute of the instance.

        Raises:
            ValueError: If `"_options"` key is present in `params`, which is not allowed.
        """
        self.params.validate()
        if "_options" in self.params:
            raise ValueError('"_options" is not allowed in params')

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
        upsert_hitl_detail(
            ti_id=ti_id,
            options=self.options,
            subject=self.subject,
            body=self.body,
            defaults=self.defaults,
            multiple=self.multiple,
            params=self.serialized_params,
            assigned_users=self.assigned_users,
        )

        if self.execution_timeout:
            timeout_datetime = utcnow() + self.execution_timeout
        else:
            timeout_datetime = None

        self.log.info("Waiting for response")
        for notifier in self.notifiers:
            notifier(context)

        # Defer the Human-in-the-loop response checking process to HITLTrigger
        self.defer(
            trigger=HITLTrigger(
                ti_id=ti_id,
                options=self.options,
                defaults=self.defaults,
                params=self.serialized_params,
                multiple=self.multiple,
                timeout_datetime=timeout_datetime,
            ),
            method_name="execute_complete",
        )

    @property
    def serialized_params(self) -> dict[str, dict[str, Any]]:
        if not AIRFLOW_V_3_1_3_PLUS:
            return self.params.dump() if isinstance(self.params, ParamsDict) else self.params
        return {k: self.params.get_param(k).serialize() for k in self.params}

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
            responded_at=event["responded_at"],
            responded_by_user=event["responded_by_user"],
        )

    def process_trigger_event_error(self, event: dict[str, Any]) -> None:
        if event["error_type"] == "timeout":
            raise HITLTimeoutError(event)

        raise HITLTriggerEventError(event)

    def validate_chosen_options(self, chosen_options: list[str]) -> None:
        """Check whether user provide valid response."""
        if diff := set(chosen_options) - set(self.options):
            raise ValueError(f"Responses {diff} not in {self.options}")

    def validate_params_input(self, params_input: Mapping) -> None:
        """Check whether user provide valid params input."""
        if self.params and params_input and set(self.serialized_params.keys()) ^ set(params_input):
            raise ValueError(f"params_input {params_input} does not match params {self.params}")

        for key, value in params_input.items():
            self.params[key] = value

    def generate_link_to_ui(
        self,
        *,
        task_instance: RuntimeTaskInstanceProtocol,
        base_url: str | None = None,
        options: str | list[str] | None = None,
        params_input: dict[str, Any] | None = None,
    ) -> str:
        """
        Generate a URL link to the "required actions" page for a specific task instance.

        This URL includes query parameters based on allowed options and parameters.

        Args:
            task_instance: The task instance to generate the link for.
            base_url: Optional base URL to use. Defaults to ``api.base_url`` from config.
            options: Optional subset of allowed options to include in the URL.
            params_input: Optional subset of allowed params to include in the URL.

        Raises:
            ValueError: If any provided option or parameter is invalid.
            ValueError: If no base_url can be determined.

        Returns:
            The full URL pointing to the required actions page with query parameters.
        """
        query_param: dict[str, Any] = {}
        options = [options] if isinstance(options, str) else options
        if options:
            if diff := set(options) - set(self.options):
                raise ValueError(f"options {diff} are not valid options")
            query_param["_options"] = options

        if params_input:
            if diff := set(params_input.keys()) - set(self.params.keys()):
                raise ValueError(f"params {diff} are not valid params")
            query_param.update(params_input)

        if not (base_url := base_url or conf.get("api", "base_url", fallback=None)):
            raise ValueError("Not able to retrieve base_url")

        query_param["map_index"] = task_instance.map_index

        parsed_base_url: ParseResult = urlparse(base_url)
        return urlunparse(
            (
                parsed_base_url.scheme,
                parsed_base_url.netloc,
                f"/dags/{task_instance.dag_id}/runs/{task_instance.run_id}/tasks/{task_instance.task_id}/required_actions",
                "",
                urlencode(query_param) if query_param else "",
                "",
            )
        )

    @staticmethod
    def generate_link_to_ui_from_context(
        *,
        context: Context,
        base_url: str | None = None,
        options: list[str] | None = None,
        params_input: dict[str, Any] | None = None,
    ) -> str:
        """
        Generate a "required actions" page URL from a task context.

        Delegates to ``generate_link_to_ui`` using the task and task_instance extracted from
        the provided context.

        Args:
            context: The Airflow task context containing 'task' and 'task_instance'.
            base_url: Optional base URL to use.
            options: Optional list of allowed options to include.
            params_input: Optional dictionary of allowed parameters to include.

        Returns:
            The full URL pointing to the required actions page with query parameters.
        """
        hitl_op = context["task"]
        if not isinstance(hitl_op, HITLOperator):
            raise ValueError("This method only supports HITLOperator")

        return hitl_op.generate_link_to_ui(
            task_instance=context["task_instance"],
            base_url=base_url,
            options=options,
            params_input=params_input,
        )


class ApprovalOperator(HITLOperator, SkipMixin):
    """Human-in-the-loop Operator that has only 'Approval' and 'Reject' options."""

    inherits_from_skipmixin = True

    FIXED_ARGS = ["options", "multiple"]

    APPROVE = "Approve"
    REJECT = "Reject"

    def __init__(
        self,
        *,
        ignore_downstream_trigger_rules: bool = False,
        fail_on_reject: bool = False,
        **kwargs,
    ) -> None:
        """
        Human-in-the-loop Operator for simple approval workflows.

        This operator presents the user with two fixed options: "Approve" and "Reject".

        Behavior:
        - "Approve": Downstream tasks execute as normal.
        - "Reject":
            - Downstream tasks are skipped according to the `ignore_downstream_trigger_rules` setting.
            - If `fail_on_reject=True`, the task fails instead of only skipping downstream tasks.

        Warning:
            Using `fail_on_reject=True` is generally discouraged. A HITLOperator's role is to collect
            human input, and receiving any response—including "Reject"—indicates the task succeeded.
            Treating "Reject" as a task failure mixes human decision outcomes with Airflow task
            success/failure states.
            Only use this option if you explicitly intend for a "Reject" response to fail the task.

        Args:
            ignore_downstream_trigger_rules: If True, skips all downstream tasks regardless of trigger rules.
            fail_on_reject: If True, the task fails when "Reject" is selected. Generally discouraged.
                Read the warning carefully before using.
        """
        for arg in self.FIXED_ARGS:
            if arg in kwargs:
                raise ValueError(f"Passing {arg} to ApprovalOperator is not allowed.")

        self.ignore_downstream_trigger_rules = ignore_downstream_trigger_rules
        self.fail_on_reject = fail_on_reject

        super().__init__(
            options=[self.APPROVE, self.REJECT],
            multiple=False,
            **kwargs,
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        ret = super().execute_complete(context=context, event=event)

        chosen_option = ret["chosen_options"][0]
        if chosen_option == self.APPROVE:
            self.log.info("Approved. Proceeding with downstream tasks...")
            return ret

        if self.fail_on_reject and chosen_option == self.REJECT:
            raise HITLRejectException('Receive "Reject"')

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


class HITLBranchOperator(HITLOperator, BranchMixIn):
    """BranchOperator based on Human-in-the-loop Response."""

    inherits_from_skipmixin = True

    def __init__(self, *, options_mapping: dict[str, str] | None = None, **kwargs) -> None:
        """
        Initialize HITLBranchOperator.

        Args:
            options_mapping:
                A dictionary mapping option labels (must match entries in `self.options`)
                to string values (e.g., task IDs). Defaults to an empty dict if not provided.

        Raises:
            ValueError:
                - If `options_mapping` contains keys not present in `self.options`.
                - If any value in `options_mapping` is not a string.
        """
        super().__init__(**kwargs)
        self.options_mapping = options_mapping or {}
        self.validate_options_mapping()

    def validate_options_mapping(self) -> None:
        """
        Validate that `options_mapping` keys match `self.options` and all values are strings.

        Raises:
            ValueError: If any key is not in `self.options` or any value is not a string.
        """
        if not self.options_mapping:
            return

        # Validate that the choice options are keys in the mapping are the same
        invalid_keys = set(self.options_mapping.keys()) - set(self.options)
        if invalid_keys:
            raise ValueError(
                f"`options_mapping` contains keys that are not in `options`: {sorted(invalid_keys)}"
            )

        # validate that all values are strings
        invalid_entries = {
            k: (v, type(v).__name__) for k, v in self.options_mapping.items() if not isinstance(v, str)
        }
        if invalid_entries:
            raise ValueError(
                f"`options_mapping` values must be strings (task_ids).\nInvalid entries: {invalid_entries}"
            )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        """Execute the operator and branch based on chosen options."""
        ret = super().execute_complete(context=context, event=event)
        chosen_options = ret["chosen_options"]

        # Map options to task IDs using the mapping, fallback to original option
        chosen_options = [self.options_mapping.get(option, option) for option in chosen_options]
        return self.do_branch(context=context, branches_to_execute=chosen_options)


class HITLEntryOperator(HITLOperator):
    """Human-in-the-loop Operator that is used to accept user input through TriggerForm."""

    OK = "OK"

    def __init__(self, **kwargs) -> None:
        if "options" not in kwargs:
            kwargs["options"] = [self.OK]

            if "defaults" not in kwargs:
                kwargs["defaults"] = [self.OK]

        super().__init__(**kwargs)
