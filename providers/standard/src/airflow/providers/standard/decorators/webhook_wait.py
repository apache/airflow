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
The ``@task.webhook_wait`` decorator.

.. note:: |experimental|
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, NoReturn

from airflow.providers.common.compat.sdk import DecoratedOperator, context_merge, task_decorator_factory
from airflow.providers.standard.operators.webhooks import WaitForWebhookOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context, TaskDecorator


class _WebhookWaitDecoratedOperator(DecoratedOperator, WaitForWebhookOperator):
    """
    Wait for a delivery to an inbound webhook that matches what the decorated function returns.

    The function runs once in each try, before the task parks, and returns the wait's match. A resumed
    task does not run it again: it returns the delivery, as
    :class:`~airflow.providers.standard.operators.webhooks.WaitForWebhookOperator` does.

    :param python_callable: The function that returns the match, as a dict.
    :param op_args: The positional arguments of the function. (templated)
    :param op_kwargs: The keyword arguments of the function. (templated)
    :param webhook: The webhook's name, as listed in ``[webhooks] names``. (templated)
    :param events: The delivery events to accept, or None to accept any event. (templated)
    :param timeout: How long to wait, at most 365 days, or None for the API server's
        ``[webhooks] default_wait_timeout``.
    """

    template_fields: Sequence[str] = ("webhook", "events", "op_args", "op_kwargs")

    custom_operator_name = "@task.webhook_wait"

    def __init__(self, **kwargs: Any) -> None:
        if "match" in kwargs:
            raise TypeError(
                f"{self.custom_operator_name} takes its match from what the function returns, "
                "not from a match argument"
            )
        super().__init__(**kwargs)

    def execute(self, context: Context) -> NoReturn:
        context_merge(context, self.op_kwargs)
        match = self.python_callable(*self.op_args, **self.determine_kwargs(context))
        if not isinstance(match, dict):
            raise TypeError(
                f"The {self.custom_operator_name} function must return the match as a dict, "
                f"not {type(match).__name__}"
            )
        self.wait_for_webhook(
            webhook=self.webhook,
            events=self.events,
            match=match,
            method_name="execute_complete",
            timeout=self.timeout,
        )


def webhook_wait_task(python_callable: Callable | None = None, **kwargs: Any) -> TaskDecorator:
    """
    Wrap a function that returns a match into a task that waits for a delivery to an inbound webhook.

    The function runs once in each try and returns the wait's match, a dict that maps attribute names
    of the webhook's source type to a value or a list of values. The task then parks until a matching
    delivery resumes it, and returns that delivery as its XCom.

    .. note:: |experimental|

    :param python_callable: The function to decorate.
    :param kwargs: The arguments of
        :class:`~airflow.providers.standard.operators.webhooks.WaitForWebhookOperator` other than
        ``match``, such as ``webhook``, ``events`` and ``timeout``, and the arguments every operator takes.
    """
    # The XCom is the delivery, not the function's return value, so its return annotation, such as
    # dict, must not unroll the delivery into one XCom per key.
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=False,
        decorated_operator_class=_WebhookWaitDecoratedOperator,
        **kwargs,
    )
