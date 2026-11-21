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
"""
TaskFlow decorator for agentic LLM workflows.

The user writes a function that **returns the prompt string**. The decorator
handles hook creation, agent configuration with toolsets, multi-turn reasoning,
and output serialization.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.utils.prompt import call_arguments
from airflow.providers.common.ai.utils.validation import (
    validate_prompt,
)
from airflow.providers.common.compat.sdk import (
    SET_DURING_EXECUTION,
    DecoratedOperator,
    TaskDecorator,
    context_merge,
    determine_kwargs,
    task_decorator_factory,
)

if TYPE_CHECKING:
    from airflow.sdk import Context


class _AgentDecoratedOperator(DecoratedOperator, AgentOperator):
    """
    Wraps a callable that returns a prompt for an agentic LLM workflow.

    The user function is called at execution time to produce the prompt string.
    All other parameters (``llm_conn_id``, ``toolsets``, ``system_prompt``, etc.)
    are passed through to :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`.

    :param python_callable: A reference to a callable that returns the prompt string.
    :param op_args: Positional arguments for the callable.
    :param op_kwargs: Keyword arguments for the callable.
    :param output_processor: Called with the agent's output, as the task would return it, and
        the arguments of the callable it names (``batch``, ``ti``, ...); what it returns is the
        task's result. For a task that must return more than what the model said, such as which
        of its inputs the answer is for.
    :param error_handler: Called with the exception of a failed agent run and the arguments of
        the callable it names. It returns the output to use instead, which then goes through
        ``output_processor``, or raises. An error of the callable itself, which builds the
        prompt, is not handed over. Not supported with ``durable=True``: the journal of the
        failed run would stay behind a task that succeeded.
    """

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *AgentOperator.template_fields,
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.agent"

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        output_processor: Callable[..., Any] | None = None,
        error_handler: Callable[..., Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            prompt=SET_DURING_EXECUTION,
            **kwargs,
        )
        if error_handler is not None and self.durable:
            raise ValueError(
                "error_handler is not supported with durable=True: the journal of the failed run "
                "would stay behind a task that succeeded."
            )
        self.output_processor = output_processor
        self.error_handler = error_handler

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        self.prompt = self.python_callable(*self.op_args, **kwargs)

        validate_prompt(self.prompt, decorator_name="@task.agent")

        # Render only the prompt the callable returned. The other template fields were already
        # rendered before execute().
        # The arguments of the call, positional ones too, can be referenced by name in the prompt.
        context_merge(context, call_arguments(self.python_callable, self.op_args, self.op_kwargs))
        self.prompt = self.render_template(self.prompt, context)
        return self._run_with_callbacks(context, lambda: AgentOperator.execute(self, context))

    def resume_after_tool_approval(self, context: Context, *args: Any, **kwargs: Any) -> Any:
        return self._run_with_callbacks(
            context, lambda: AgentOperator.resume_after_tool_approval(self, context, *args, **kwargs)
        )

    def resume_hitl_review(self, context: Context, *args: Any, **kwargs: Any) -> Any:
        return self._run_with_callbacks(
            context, lambda: AgentOperator.resume_hitl_review(self, context, *args, **kwargs)
        )

    def _run_with_callbacks(self, context: Context, run: Callable[[], Any]) -> Any:
        """
        Return the result of ``run``, through ``error_handler`` and ``output_processor`` when set.

        Every way a run ends goes through here: ``execute``, and the methods a run paused for
        tool approval or review resumes in. A pause is not an error: it is a ``BaseException``
        and passes ``error_handler`` by.
        """
        try:
            output = run()
        except Exception as error:
            if self.error_handler is None:
                raise
            output = self._call_with_arguments(self.error_handler, error, context)
            # The failed run saved its usage for a retry that will not come: the task succeeds.
            self._clear_usage_budget(context)
        if self.output_processor is None:
            return output
        return self._call_with_arguments(self.output_processor, output, context)

    def _call_with_arguments(self, function: Callable[..., Any], value: Any, context: Context) -> Any:
        """Call ``function(value, ...)`` with the arguments of the task's callable that it names."""
        # A resumed run starts from a fresh context: the arguments are merged into it again.
        context_merge(context, call_arguments(self.python_callable, self.op_args, self.op_kwargs))
        return function(value, **determine_kwargs(function, (value,), context))


def agent_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Wrap a function that returns a prompt into an agentic LLM task.

    The function body constructs the prompt (can use Airflow context, XCom, etc.).
    The decorator handles hook creation, agent configuration with toolsets,
    multi-turn reasoning, and output serialization.

    Usage::

        @task.agent(
            llm_conn_id="pydanticai_default",
            system_prompt="You are a data analyst.",
            toolsets=[SQLToolset(db_conn_id="postgres_default")],
        )
        def analyze(question: str):
            return "Answer: {{ question }}"

    :param python_callable: Function to decorate.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_AgentDecoratedOperator,
        **kwargs,
    )
