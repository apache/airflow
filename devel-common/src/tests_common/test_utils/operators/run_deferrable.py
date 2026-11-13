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

import asyncio
from copy import deepcopy
from importlib import import_module
from typing import TYPE_CHECKING, Any

from airflow.exceptions import TaskDeferred

from tests_common.test_utils.mock_context import mock_context

if TYPE_CHECKING:
    from airflow.models import Operator
    from airflow.triggers.base import BaseTrigger, TriggerEvent


async def run_tigger(trigger: BaseTrigger) -> list[TriggerEvent]:
    events = []
    async for event in trigger.run():
        events.append(event)
    return events


def run_trigger(trigger: BaseTrigger) -> list[TriggerEvent]:
    return asyncio.run(run_tigger(trigger))


def execute_operator(operator: Operator) -> tuple[Any, Any]:
    context = mock_context(task=operator)
    return asyncio.run(deferrable_operator(context, operator))


def start_from_trigger(context, operator) -> TaskDeferred:
    """
    Defer an operator which starts from the triggerer, as the scheduler and the triggerer do.

    The trigger is built from the arguments the operator declared when it was created, so before
    any of its templated fields got rendered, and the triggerer then renders the fields the
    trigger shares with the operator.
    """
    start_trigger_args = operator.start_trigger_args
    trigger_kwargs = start_trigger_args.trigger_kwargs or {}
    module_name, _, class_name = start_trigger_args.trigger_cls.rpartition(".")
    trigger = getattr(import_module(module_name), class_name)(**trigger_kwargs)
    trigger.template_fields = tuple(
        field for field in operator.template_fields if field in trigger_kwargs and hasattr(trigger, field)
    )
    trigger.render_template_fields(context=context)
    return TaskDeferred(
        trigger=trigger,
        method_name=start_trigger_args.next_method,
        kwargs=start_trigger_args.next_kwargs,
        timeout=start_trigger_args.timeout,
    )


async def deferrable_operator(context, operator):
    result = None
    triggered_events = []
    try:
        if getattr(operator, "start_from_trigger", False):
            deferred = start_from_trigger(context, operator)
            # The worker only gets involved once the trigger fired, and renders the operator then.
            operator.render_template_fields(context=context)
            raise deferred
        operator.render_template_fields(context=context)
        result = operator.execute(context=context)
    except TaskDeferred as deferred:
        task: TaskDeferred | None = deferred

        while task:
            events = await run_tigger(task.trigger)

            if not events:
                break

            triggered_events.extend(deepcopy(events))

            try:
                method = getattr(operator, task.method_name)
                result = method(context=context, event=next(iter(events)).payload)
                task = None
            except TaskDeferred as exception:
                task = exception
    return result, triggered_events
