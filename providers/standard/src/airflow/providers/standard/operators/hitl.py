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
import warnings

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.standard.version_compat import (
    AIRFLOW_V_3_1_3_PLUS,
    AIRFLOW_V_3_1_PLUS,
    AIRFLOW_V_3_3_PLUS,
    AIRFLOW_V_3_4_PLUS,
)

if not AIRFLOW_V_3_1_PLUS:
    raise AirflowOptionalProviderFeatureException("Human in the loop functionality needs Airflow 3.1+.")

from collections.abc import Collection, Mapping, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any, NoReturn
from urllib.parse import ParseResult, urlencode, urlparse, urlunparse

from airflow.providers.common.compat.sdk import BranchMixIn, SkipMixin, conf, get_current_context
from airflow.providers.standard.exceptions import (  # noqa: F401 - kept importable from this module
    HITLRejectException,
    HITLTimeoutError,
    HITLTriggerEventError,
)
from airflow.providers.standard.operators.python import PythonOperator, _BasePythonVirtualenvOperator
from airflow.providers.standard.triggers.hitl import HITLTrigger, HITLTriggerEventSuccessPayload
from airflow.providers.standard.utils.hitl import (
    InputResponse,
    answer_log_var,
    raise_for_error_event,
    validate_chosen_options,
    validate_defaults,
    validate_options,
    validate_params,
    validate_params_input,
)
from airflow.providers.standard.version_compat import BaseOperator, is_arg_set
from airflow.sdk.bases.notifier import BaseNotifier
from airflow.sdk.definitions.param import ParamsDict
from airflow.sdk.execution_time.hitl import upsert_hitl_detail
from airflow.sdk.timezone import utcnow

if AIRFLOW_V_3_3_PLUS:
    # On Airflow 3.3+ the operator parks the task in the first-class AWAITING_INPUT state instead of
    # deferring to a trigger. On older Airflow versions this name is absent and the defer() fallback is used.
    from airflow.sdk.exceptions import TaskAwaitingInput

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.providers.common.compat.sdk import Context
    from airflow.sdk.execution_time.hitl import HITLUser
    from airflow.sdk.types import RuntimeTaskInstanceProtocol


def _request_input_and_wait(
    task: BaseOperator,
    context: Context,
    *,
    options: list[str],
    subject: str,
    body: str | None,
    defaults: list[str] | None,
    multiple: bool,
    params: dict[str, Any],
    assigned_users: list[HITLUser] | None,
    notifiers: Sequence[BaseNotifier],
    timeout: timedelta | None,
    timeout_datetime: datetime | None,
    method_name: str,
    kwargs: dict[str, Any] | None = None,
) -> NoReturn:
    """Write the Human-in-the-loop request, notify, and stop the task until a response arrives."""
    ti_id = context["task_instance"].id
    upsert_hitl_detail(
        ti_id=ti_id,
        options=options,
        subject=subject,
        body=body,
        defaults=defaults,
        multiple=multiple,
        params=params,
        assigned_users=assigned_users,
    )

    task.log.info("Waiting for response")
    for notifier in notifiers:
        notifier(context)

    if AIRFLOW_V_3_3_PLUS:
        # Airflow 3.3+: park the task in AWAITING_INPUT -- no trigger, no triggerer. The task
        # is resumed by the Core API response handler or the scheduler timeout sweep, so the
        # triggerer no longer needs to run for Human-in-the-loop tasks to make progress.
        raise TaskAwaitingInput(method_name=method_name, kwargs=kwargs, timeout=timeout)

    # Fallback for Airflow versions < 3.3: defer the response check to HITLTrigger on the triggerer.
    task.defer(
        trigger=HITLTrigger(
            ti_id=ti_id,
            options=options,
            defaults=defaults,
            params=params,
            multiple=multiple,
            timeout_datetime=timeout_datetime,
        ),
        method_name=method_name,
        kwargs=kwargs,
    )


def request_input(
    *,
    subject: str,
    options: list[str],
    body: str | None = None,
    defaults: str | list[str] | None = None,
    multiple: bool = False,
    params: ParamsDict | dict[str, Any] | None = None,
    notifiers: Sequence[BaseNotifier] | BaseNotifier | None = None,
    assigned_users: HITLUser | list[HITLUser] | None = None,
    timeout: timedelta | None = None,
    key: str | None = None,
) -> InputResponse:
    """
    Ask a person for input from inside a running Python task, and return their answer.

    The first time a question is asked, the request appears on the Required Actions page, the same
    as one from :class:`HITLOperator`, and the task stops there: it waits in the ``awaiting_input``
    state without holding a worker slot. Once the request is answered, the task runs again from the
    start of its callable, and this call returns the answer instead of asking again. Save anything
    the second run should not redo (for example an agent framework's run state) to
    ``task_state_store`` before calling this.

    It works in ``@task`` and ``PythonOperator`` callables (sync or async), including ``@task.branch``
    and ``@task.short_circuit``. It can't pause tasks that run their callable in another process, such
    as ``@task.virtualenv``, ``@task.external_python``, ``@task.docker`` or ``@task.kubernetes``.
    Ask one question at a time from the task's own code, not from worker threads or from several
    ``async`` calls running at once: a task instance has a single Required Actions request.

    Needs Airflow 3.4+. Earlier versions keep the first request's subject, body and options when a
    task asks again, after a resume or on a retry, so the reviewer would answer a question they were
    not shown.

    :param subject: Headline shown to the reviewer.
    :param options: Options the reviewer chooses from.
    :param body: Details the reviewer needs to decide (Markdown).
    :param defaults: Options applied if ``timeout`` passes without an answer. Without defaults, the
        task fails with :class:`~airflow.providers.standard.exceptions.HITLTimeoutError` at the timeout.
    :param multiple: Whether the reviewer can choose more than one option.
    :param params: Form fields for the reviewer to fill in, defined like Dag params.
    :param notifiers: Notifiers called with the task context once the request is written.
    :param assigned_users: Users allowed to answer. Anyone with access can answer if not set.
    :param timeout: How long to wait for an answer. Waits indefinitely if not set.
    :param key: Which question this is, so a resumed task gets each answer back at the right call.
        Defaults to ``subject``. Each key is asked at most once per try. Set it when the subject can
        change between runs, or when the task can ask the same subject more than once, for example
        one approval per agent tool call (use the call's id).
    """
    if not AIRFLOW_V_3_4_PLUS:
        raise AirflowOptionalProviderFeatureException("request_input() needs Airflow 3.4+.")
    context = get_current_context()
    task = context["task"]
    if not isinstance(task, PythonOperator) or isinstance(task, _BasePythonVirtualenvOperator):
        raise TypeError(
            f"request_input() was called from {type(task).__name__}, which cannot resume it. It works "
            "only in tasks that run their Python callable in the task's own process, such as @task "
            "and PythonOperator."
        )
    defaults = [defaults] if isinstance(defaults, str) else defaults
    params = params if isinstance(params, ParamsDict) else ParamsDict(params or {})
    validate_options(options)
    validate_params(params)
    validate_defaults(defaults, options, multiple)
    key = subject if key is None else key
    answer_log = answer_log_var.get()
    if answer_log is not None and (answer := answer_log.take(key)) is not None:
        # The code may have changed since the reviewer answered; never hand back an option it no
        # longer offers.
        validate_chosen_options(answer["chosen_options"], options)
        return answer
    serialized_params = {name: params.get_param(name).serialize() for name in params}

    _request_input_and_wait(
        task,
        context,
        options=options,
        subject=subject,
        body=body,
        defaults=defaults,
        multiple=multiple,
        params=serialized_params,
        assigned_users=[assigned_users] if isinstance(assigned_users, dict) else assigned_users,
        notifiers=[notifiers] if isinstance(notifiers, BaseNotifier) else notifiers or [],
        timeout=timeout,
        timeout_datetime=None,
        method_name="resume_with_input",
        kwargs={
            "key": key,
            # What the resume checks the answer against.
            "options": options,
            # A param without a default serializes its value as NOTSET, which can't be stored with
            # the paused task. The resume only checks the answer against each schema, so None will do.
            "params": {
                name: {**spec, "value": spec["value"] if is_arg_set(spec["value"]) else None}
                for name, spec in serialized_params.items()
            },
            # Earlier answers travel with the task so each run can replay every one of them.
            "answered": [[answered_key, earlier] for answered_key, earlier in answer_log.answers.items()]
            if answer_log is not None
            else [],
        },
    )


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
    :param response_timeout: Maximum time to wait for a human response. On Airflow 3.3+ this is
        enforced by the scheduler's ``awaiting_input`` timeout sweep; on older versions it is passed
        to the triggerer. This is separate from ``execution_timeout``, which controls the execution
        phase before the task starts waiting. If not set, no timeout is applied to the human response wait.
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
        response_timeout: timedelta | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        # Handle backward compatibility: if execution_timeout is set but response_timeout is not,
        # migrate execution_timeout to response_timeout and clear it to prevent the BaseOperator
        # timeout from racing the defer() call.
        if self.execution_timeout and not response_timeout:
            warnings.warn(
                "Passing `execution_timeout` to HITLOperator to control the human response wait is "
                "deprecated. Use `response_timeout` instead. `execution_timeout` will be cleared to "
                "prevent it from killing the task before defer() is reached.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            response_timeout = self.execution_timeout
            self.execution_timeout = None

        self.response_timeout = response_timeout
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

        # Runtime/subclass additions to the summary; config-derived entries live in the property.
        self.hitl_summary_extra: dict[str, Any] = {}

    @property
    def hitl_summary(self) -> dict[str, Any]:
        """
        Summary of the Human-in-the-loop request, for listeners/observability.

        A property so the ``subject``/``body`` template fields are read after rendering, not
        captured as un-rendered Jinja in ``__init__``. Each access builds a fresh snapshot, so
        mutating the returned dict has no effect — runtime code and subclasses extend the summary
        by adding entries to ``hitl_summary_extra`` instead.
        """
        return {
            "subject": self.subject,
            "body": self.body,
            "options": self.options,
            "defaults": self.defaults,
            "multiple": self.multiple,
            "assigned_users": self.assigned_users,
            "serialized_params": self.serialized_params or None,
            **self.hitl_summary_extra,
        }

    def validate_options(self) -> None:
        """
        Validate the `options` attribute of the instance.

        Raises:
            ValueError: If `options` is empty.
        """
        validate_options(self.options)

    def validate_params(self) -> None:
        """
        Validate the `params` attribute of the instance.

        Note: Value validation (e.g., required fields, schema) is intentionally skipped here
        because HITLOperator params represent form fields that are filled by a human at runtime.
        Values do not exist at Dag parse time, so validating them in ``__init__`` would cause
        a ``ParamValidationError`` for any param without a default. Value validation happens
        in ``validate_params_input`` after the human submits the form.

        Raises:
            ValueError: If ``"_options"`` key is present in ``params``, which is not allowed.
        """
        validate_params(self.params)

    def validate_defaults(self) -> None:
        """
        Validate whether the given defaults pass the following criteria.

        1. Default options should be the subset of options.
        2. When multiple is False, there should only be one option.
        """
        validate_defaults(self.defaults, self.options, self.multiple)

    def execute(self, context: Context):
        """
        Write the Human-in-the-loop request, then wait for a user response.

        On Airflow 3.3+ the task waits in the ``awaiting_input`` state with no trigger or triggerer
        involved; on older versions it defers to :class:`HITLTrigger`. Either way it resumes in
        ``execute_complete`` once a response (or timeout default) arrives.
        """
        if self.response_timeout:
            timeout_datetime = utcnow() + self.response_timeout
        else:
            timeout_datetime = None

        # Enrich summary with runtime info
        self.hitl_summary_extra["timeout_datetime"] = (
            timeout_datetime.isoformat() if timeout_datetime else None
        )

        _request_input_and_wait(
            self,
            context,
            options=self.options,
            subject=self.subject,
            body=self.body,
            defaults=self.defaults,
            multiple=self.multiple,
            params=self.serialized_params,
            assigned_users=self.assigned_users,
            notifiers=self.notifiers,
            timeout=self.response_timeout,
            timeout_datetime=timeout_datetime,
            method_name="execute_complete",
        )

    @property
    def serialized_params(self) -> dict[str, dict[str, Any]]:
        if not AIRFLOW_V_3_1_3_PLUS:
            return self.params.dump() if isinstance(self.params, ParamsDict) else self.params
        return {k: self.params.get_param(k).serialize() for k in self.params}

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        if "error" in event:
            self.hitl_summary_extra["error_type"] = event["error_type"]
            self.process_trigger_event_error(event)

        chosen_options = event["chosen_options"]
        params_input = event["params_input"] or {}
        self.validate_chosen_options(chosen_options)
        self.validate_params_input(params_input)

        self.hitl_summary_extra.update(
            {
                "chosen_options": chosen_options,
                "params_input": params_input,
                "responded_at": event["responded_at"].isoformat(),
                "responded_by_user": event["responded_by_user"],
            }
        )

        return HITLTriggerEventSuccessPayload(
            chosen_options=chosen_options,
            params_input=params_input,
            responded_at=event["responded_at"],
            responded_by_user=event["responded_by_user"],
        )

    def process_trigger_event_error(self, event: dict[str, Any]) -> None:
        raise_for_error_event(event)

    def validate_chosen_options(self, chosen_options: list[str]) -> None:
        """Check whether user provide valid response."""
        validate_chosen_options(chosen_options, self.options)

    def validate_params_input(self, params_input: Mapping) -> None:
        """Check whether user provide valid params input."""
        validate_params_input(params_input, self.params)

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

        self.hitl_summary_extra["ignore_downstream_trigger_rules"] = self.ignore_downstream_trigger_rules
        self.hitl_summary_extra["fail_on_reject"] = self.fail_on_reject

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        ret = super().execute_complete(context=context, event=event)

        chosen_option = ret["chosen_options"][0]
        self.hitl_summary_extra["approved"] = chosen_option == self.APPROVE
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
        self.hitl_summary_extra["options_mapping"] = self.options_mapping

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
        self.hitl_summary_extra["branches_to_execute"] = chosen_options
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
