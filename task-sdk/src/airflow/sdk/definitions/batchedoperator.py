#
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

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import attrs

from airflow.sdk.bases.decorator import _TaskDecorator
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.definitions._internal.expandinput import (
    DecoratedExpandInput,
    ExpandInput,
    OperatorExpandArgument,
    OperatorExpandKwargsArgument,
)
from airflow.sdk.definitions.mappedoperator import OperatorPartial
from airflow.sdk.definitions.xcom_arg import PlainXComArg, XComArg

if TYPE_CHECKING:
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.definitions.iterableoperator import MappedIterableOperator

T = TypeVar("T", bound=OperatorPartial | _TaskDecorator)


def validate_batch_size(size: int | XComArg) -> int | XComArg:
    """
    Validate the ``size`` handed to ``.batch()`` at DAG-definition time.

    A literal size must be at least 2 (``.iterate()`` covers a single task instance). A runtime
    size must be the return value of a plain, non-mapped task: the scheduler learns it from the
    ``mapped_length`` that the return value's push records on its XCom row (never from the XCom itself), so
    a ``.map()``/``.filter()`` result, a pushed key or a mapped upstream cannot provide one.
    """
    if isinstance(size, PlainXComArg):
        if size.operator.is_mapped:
            raise ValueError(f"batch size cannot come from mapped task {size.operator.task_id!r}")
        if size.key != BaseXCom.XCOM_RETURN_KEY:
            raise ValueError(
                f"batch size must be the return value of {size.operator.task_id!r}, not its {size.key!r} XCom"
            )
        return size
    if isinstance(size, XComArg):
        raise TypeError(f"batch size must be a plain XComArg, not {type(size).__name__}")
    if size < 2:
        raise ValueError(f"batch size must be at least 2, got {size}")
    return size


@attrs.define(kw_only=True, repr=False)
class BatchableOperator(Generic[T], metaclass=ABCMeta):
    """
    Intermediate abstraction for batched mapping.

    This class decorates an OperatorPartial and stores configuration for batched mapping.
    It is used to facilitate batched expansion of operators, allowing tasks to be mapped over batches
    of data and then iterate over the batched data.

    :param operator_partial: The partial operator to be batched.
    :param size: The number of task instances to create. The input is distributed across them
        round-robin (item ``i`` goes to task instance ``i % size``), not split into ``size``
        contiguous chunks — this is *not* the same semantics as ``itertools.batched(iterable, size)``.
        See :class:`~airflow.sdk.definitions._internal.expandinput.BatchedExpandInput` for why
        round-robin is used instead of contiguous chunking. Exactly ``size`` task instances are
        always created; if the input yields fewer than ``size`` items, the surplus instances run
        with no items and succeed immediately. May be an ``XComArg`` whose integer value is only
        known at run time: the scheduler then creates that many task instances and each of them
        resolves the same XCom to pick its share.
    """

    operator_partial: T
    size: int | XComArg

    @property
    def operator_class(self) -> type[BaseOperator]:
        return self.operator_partial.operator_class

    @property
    def kwargs(self) -> dict[str, Any]:
        return self.operator_partial.kwargs

    @abstractmethod
    def iterate(self, **mapped_kwargs: OperatorExpandArgument) -> Any:
        """
        Iterate the operator over the provided mapped keyword arguments.

        :param mapped_kwargs: Keyword arguments to expand against.
        :return: An expanded operator or XComArg, depending on the subclass implementation.
        """

    @abstractmethod
    def iterate_kwargs(self, kwargs: OperatorExpandKwargsArgument, *, strict: bool = True) -> Any:
        """
        Iterate the operator over a list of dictionaries or XComArg.

        :param kwargs: List of dicts or XComArg to expand against.
        :param strict: Whether to enforce strict argument checking.
        :return: An expanded operator or XComArg, depending on the subclass implementation.
        """

    @abstractmethod
    def _iterate(self, expand_input: ExpandInput, *, strict: bool) -> MappedIterableOperator:
        """
        Build the ``MappedIterableOperator`` that spreads ``expand_input`` over ``size`` task instances.

        The wrapped partial's ``_expand`` builds the in-memory ``MappedOperator`` (never registered
        with the DAG), exactly as its own ``.iterate()`` does for a single task instance.

        :param expand_input: The input to iterate against.
        :param strict: Whether to enforce strict argument checking.
        """


@attrs.define(kw_only=True, repr=False)
class BatchedOperator(BatchableOperator[OperatorPartial]):
    """
    Concrete implementation of BatchableOperator for classic (non-decorated) operators.

    This class wraps an OperatorPartial and provides batched expansion and iteration logic
    for classic Airflow operators. It enables mapping tasks over batches of data, supporting
    both direct expansion via keyword arguments and expansion via a list of dictionaries or XComArg.

    :param operator_partial: The OperatorPartial instance to be batched and expanded.
    :param size: The number of task instances to create for mapping. Items are distributed across
        them round-robin (item ``i`` goes to task instance ``i % size``), not split into ``size``
        contiguous chunks. Exactly ``size`` task instances are always created, even when the input
        yields fewer items.
    """

    def iterate(self, **mapped_kwargs: OperatorExpandArgument) -> MappedIterableOperator:
        # Since the input is already checked at parse time, we can set strict
        # to False to skip the checks on execution.
        return self._iterate(self.operator_partial._iterate_input(**mapped_kwargs), strict=False)

    def iterate_kwargs(
        self, kwargs: OperatorExpandKwargsArgument, *, strict: bool = True
    ) -> MappedIterableOperator:
        return self._iterate(self.operator_partial._iterate_kwargs_input(kwargs), strict=strict)

    def _iterate(self, expand_input: ExpandInput, *, strict: bool) -> MappedIterableOperator:
        from airflow.sdk.definitions.iterableoperator import MappedIterableOperator

        operator = self.operator_partial._expand(expand_input, strict=strict, register_with_dag=False)
        return MappedIterableOperator(
            mapped_operator=operator, expand_input=expand_input, batch_size=self.size
        )


@attrs.define(kw_only=True, repr=False)
class DecoratedBatchedOperator(BatchableOperator[_TaskDecorator]):
    """
    Concrete implementation of BatchableOperator for decorated (TaskFlow) operators.

    This class wraps a _TaskDecorator and provides batched expansion and iteration logic
    for TaskFlow-style decorated Airflow operators. It enables mapping decorated tasks over
    batches of data, returning XComArg objects for downstream dependencies and supporting
    both direct expansion via keyword arguments and expansion via a list of dictionaries or XComArg.

    :param operator_partial: The _TaskDecorator instance to be batched and expanded.
    :param size: The number of task instances to create for mapping. Items are distributed across
        them round-robin (item ``i`` goes to task instance ``i % size``), not split into ``size``
        contiguous chunks. Exactly ``size`` task instances are always created, even when the input
        yields fewer items.
    """

    def iterate(self, **map_kwargs: OperatorExpandArgument) -> XComArg:
        # Since the input is already checked at parse time, we can set strict
        # to False to skip the checks on execution.
        return XComArg(
            operator=self._iterate(self.operator_partial._iterate_input(**map_kwargs), strict=False)
        )

    def iterate_kwargs(self, kwargs: OperatorExpandKwargsArgument, *, strict: bool = True) -> XComArg:
        return XComArg(
            operator=self._iterate(self.operator_partial._iterate_kwargs_input(kwargs), strict=strict)
        )

    def _iterate(self, expand_input: ExpandInput, *, strict: bool) -> MappedIterableOperator:
        from airflow.sdk.definitions.iterableoperator import MappedIterableOperator

        operator = self.operator_partial._expand(expand_input, strict=strict, register_with_dag=False)
        return MappedIterableOperator(
            mapped_operator=operator,
            expand_input=DecoratedExpandInput(expand_input),
            batch_size=self.size,
        )
