from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from airflow.decorators.base import Task, _TaskDecorator
from airflow.exceptions import AirflowSkipException

if TYPE_CHECKING:
    from airflow.models.baseoperator import TaskPreExecuteHook
    from airflow.utils.context import Context

__all__ = ["run_if", "skip_if"]

_T = TypeVar("_T", bound="Task[..., Any] | _TaskDecorator[..., Any, Any]")


def run_if(condition: Callable[[Context], bool]) -> Callable[[_T], _T]:
    """
    Decorate a task to run only if a condition is met.

    :param condition: A function that takes a context and returns a boolean.
    """
    wrapped_condition = wrap_skip(condition, "run if", reverse=True)  # FIXME: error message

    def decorator(task: _T) -> _T:
        if not isinstance(task, _TaskDecorator):
            error_msg = "run_if can only be used with task. decorate with @task before @run_if."
            raise TypeError(error_msg)

        pre_execute: TaskPreExecuteHook | None = task.kwargs.get("pre_execute")
        new_pre_execute = combine_hooks(pre_execute, wrapped_condition)
        task.kwargs["pre_execute"] = new_pre_execute
        return task

    return decorator


def skip_if(condition: Callable[[Context], bool]) -> Callable[[_T], _T]:
    """
    Decorate a task to skip if a condition is met.

    :param condition: A function that takes a context and returns a boolean.
    """
    wrapped_condition = wrap_skip(condition, "skip if", reverse=False)  # FIXME: error message

    def decorator(task: _T) -> _T:
        if not isinstance(task, _TaskDecorator):
            error_msg = "skip_if can only be used with task. decorate with @task before @skip_if."
            raise TypeError(error_msg)

        pre_execute: TaskPreExecuteHook | None = task.kwargs.get("pre_execute")
        new_pre_execute = combine_hooks(pre_execute, wrapped_condition)
        task.kwargs["pre_execute"] = new_pre_execute
        return task

    return decorator


def wrap_skip(func: Callable[[Context], bool], error_msg: str, *, reverse: bool) -> TaskPreExecuteHook:
    @wraps(func)
    def pre_execute(context: Context) -> None:
        condition = func(context)
        if reverse:
            condition = not condition
        if condition:
            raise AirflowSkipException(error_msg)  # FIXME

    return pre_execute


def combine_hooks(*hooks: TaskPreExecuteHook | None) -> TaskPreExecuteHook:
    def pre_execute(context: Context) -> None:
        for hook in hooks:
            if hook is None:
                continue
            hook(context)

    return pre_execute
