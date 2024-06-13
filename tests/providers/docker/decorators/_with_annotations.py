from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.decorators import task

if TYPE_CHECKING:
    from airflow.decorators.base import Task

__all__ = []


def create_task_factory(image: str, **kwargs: Any) -> Task[..., Any]:  # type: ignore  # noqa: F821
    kwargs.setdefault("multiple_outputs", False)

    @task.docker(image=image, auto_remove="force", **kwargs)
    def func(value: dict[str, Any]) -> Any:  # type: ignore  # noqa: F821
        assert isinstance(value, dict)

    return func
