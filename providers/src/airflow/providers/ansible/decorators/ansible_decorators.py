from __future__ import annotations
import warnings
from typing import Any, Collection, Mapping, Sequence, Callable
from airflow.utils.context import Context, context_merge
from airflow.utils.operator_helpers import determine_kwargs
from airflow.decorators.base import (
    DecoratedOperator,
    TaskDecorator,
    task_decorator_factory,
)
from airflow.providers.ansible.operators.ansible_operator import AnsibleOperator


class AnsibleDecoratedOperator(DecoratedOperator, AnsibleOperator):
    """Ansible Decorated Operator"""

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *AnsibleOperator.template_fields,
    )
    template_fields_renderers: dict[str, str] = {
        **DecoratedOperator.template_fields_renderers,
        **AnsibleOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.ansible"

    def __init__(
        self,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.pop("multiple_outputs", None):
            warnings.warn(
                f"`multiple_outputs=True` is not supported in {self.custom_operator_name} tasks. Ignoring.",
                UserWarning,
                stacklevel=3,
            )
        for k in ("ansible_vars",):  # Unsupported parameters in the decorator
            if k in kwargs:
                kwargs.pop(k)
        for k in AnsibleOperator.operator_fields:
            # When k is not in kwargs, pass op_kwargs to AnsibleOperator
            if (
                k not in ("ssh_key", "pool", "queue")  # Unsupported parameters in the decorator
                and k not in kwargs
                and k in op_kwargs
            ):
                kwargs[k] = op_kwargs.get(k)
        self.log.debug("AnsibleDecoratedOperator op_args: %s", op_args)
        self.log.debug("AnsibleDecoratedOperator op_kwargs: %s", op_kwargs)
        self.log.debug("AnsibleDecoratedOperator kwargs: %s", kwargs)
        super().__init__(
            kwargs_to_upstream={
                "op_args": op_args,
                "op_kwargs": op_kwargs,
            },
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )
        self.log.debug("AnsibleDecoratedOperator kwargs: %s", kwargs)

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        self.log.debug("AnsibleDecoratedOperator.execute op_kwargs: %s", self.op_kwargs)
        self.log.debug("AnsibleDecoratedOperator.execute op_args: %s", self.op_args)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)
        self.log.debug("AnsibleDecoratedOperator.execute kwargs: %s", kwargs)
        super().execute(context)
        return self.python_callable(*self.op_args, **kwargs)


def ansible_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """Ansible task"""
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=AnsibleDecoratedOperator,
        **kwargs,
    )
