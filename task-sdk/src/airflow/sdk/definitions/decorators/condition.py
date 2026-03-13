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

from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, Any, TypeVar

from airflow.sdk.bases.decorator import Task, _TaskDecorator
from airflow.sdk.exceptions import AirflowSkipException

if TYPE_CHECKING:
    from typing import TypeAlias

    from airflow.sdk.bases.operator import TaskPreExecuteHook
    from airflow.sdk.definitions.context import Context

    BoolConditionFunc: TypeAlias = Callable[[Context], bool]
    MsgConditionFunc: TypeAlias = Callable[[Context], tuple[bool, str | None]]
    AnyConditionFunc: TypeAlias = BoolConditionFunc | MsgConditionFunc

__all__ = ["run_if", "skip_if"]

_T = TypeVar("_T", bound="Task[..., Any] | _TaskDecorator[..., Any, Any]")


def run_if(condition: AnyConditionFunc, skip_message: str | None = None) -> Callable[[_T], _T]:
    """
    Decorate a task to run only if a condition is met.

    :param condition: A function that takes a context and returns a boolean.
    :param skip_message: The message to log if the task is skipped.
        If None, a default message is used.
    """
    wrapped_condition = wrap_skip(
        condition, skip_message or "Task was skipped due to condition.", reverse=True
    )

    def decorator(task: _T) -> _T:
        if not isinstance(task, _TaskDecorator):
            error_msg = "run_if can only be used with task. decorate with @task before @run_if."
            raise TypeError(error_msg)

        pre_execute: TaskPreExecuteHook | None = task.kwargs.get("pre_execute")
        new_pre_execute = combine_hooks(pre_execute, wrapped_condition)
        task.kwargs["pre_execute"] = new_pre_execute
        return task  # type: ignore[return-value]

    return decorator


def skip_if(condition: AnyConditionFunc, skip_message: str | None = None) -> Callable[[_T], _T]:
    """
    Decorate a task to skip if a condition is met.

    :param condition: A function that takes a context and returns a boolean.
    :param skip_message: The message to log if the task is skipped.
        If None, a default message is used.
    """
    wrapped_condition = wrap_skip(
        condition, skip_message or "Task was skipped due to condition.", reverse=False
    )

    def decorator(task: _T) -> _T:
        if not isinstance(task, _TaskDecorator):
            error_msg = "skip_if can only be used with task. decorate with @task before @skip_if."
            raise TypeError(error_msg)

        pre_execute: TaskPreExecuteHook | None = task.kwargs.get("pre_execute")
        new_pre_execute = combine_hooks(pre_execute, wrapped_condition)
        task.kwargs["pre_execute"] = new_pre_execute
        return task  # type: ignore[return-value]

    return decorator


def wrap_skip(func: AnyConditionFunc, error_msg: str, *, reverse: bool) -> TaskPreExecuteHook:
    @wraps(func)
    def pre_execute(context: Context) -> None:
        condition = func(context)
        skip_msg = error_msg

        if isinstance(condition, tuple):
            condition, maybe_error_msg = condition
            if maybe_error_msg:
                skip_msg = maybe_error_msg

        if reverse:
            condition = not condition
        if condition:
            raise AirflowSkipException(skip_msg)

    return pre_execute


def combine_hooks(*hooks: TaskPreExecuteHook | None) -> TaskPreExecuteHook:
    def pre_execute(context: Context) -> None:
        for hook in hooks:
            if hook is None:
                continue
            hook(context)

    return pre_execute
