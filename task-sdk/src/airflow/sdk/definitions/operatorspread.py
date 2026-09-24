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
    from airflow.sdk.definitions.iterableoperator import IterableOperator, MappedIterableOperator

T = TypeVar("T", bound=OperatorPartial | _TaskDecorator)


def validate_spread_across(across: int | XComArg) -> int | XComArg:
    """
    Validate the ``across`` handed to ``.spread()`` at DAG-definition time.

    A literal must be at least 1; 1 is a single task instance, what ``.iterate()`` alone gives, so
    a DAG can take the count from a parameter without a branch. A runtime value
    must be the return value of a plain, non-mapped task: the scheduler learns it from the
    ``mapped_length`` that the return value's push records on its XCom row (never from the XCom itself), so
    a ``.map()``/``.filter()`` result, a pushed key or a mapped upstream cannot provide one.
    """
    if isinstance(across, PlainXComArg):
        if across.operator.is_mapped:
            raise ValueError(f"across cannot come from mapped task {across.operator.task_id!r}")
        if across.key != BaseXCom.XCOM_RETURN_KEY:
            raise ValueError(
                f"across must be the return value of {across.operator.task_id!r}, not its {across.key!r} XCom"
            )
        return across
    if isinstance(across, XComArg):
        raise TypeError(f"across must be a plain XComArg, not {type(across).__name__}")
    if across < 1:
        raise ValueError(f"across must be at least 1, got {across}")
    return across


@attrs.define(kw_only=True, repr=False)
class OperatorSpread(Generic[T], metaclass=ABCMeta):
    """
    What ``.spread(across=N)`` returns: a partial waiting for ``.iterate()``, like an OperatorPartial waits for ``.expand()``.

    It wraps the OperatorPartial (or ``@task`` decorator) and remembers how many task instances
    the iteration is spread across. ``.iterate()`` / ``.iterate_kwargs()`` then build the
    ``MappedIterableOperator`` that creates those task instances, each iterating over its share of
    the input.

    :param operator_partial: The partial operator to spread.
    :param across: The number of task instances to create. The input is distributed across them
        round-robin (item ``i`` goes to task instance ``i % across``), not split into ``across``
        contiguous chunks — this is *not* the same semantics as ``itertools.batched(iterable, n)``.
        See :class:`~airflow.sdk.definitions._internal.expandinput.SpreadExpandInput` for why
        round-robin is used instead of contiguous chunking. Exactly ``across`` task instances are
        always created; if the input yields fewer items, the surplus instances run with no items
        and succeed immediately. May be an ``XComArg`` whose integer value is only known at run
        time: the scheduler then creates that many task instances and each of them resolves the
        same XCom to pick its share.
    """

    operator_partial: T
    across: int | XComArg

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
    def _iterate(
        self, expand_input: ExpandInput, *, strict: bool
    ) -> MappedIterableOperator | IterableOperator:
        """
        Build the ``MappedIterableOperator`` that spreads ``expand_input`` over ``across`` task instances.

        ``across=1`` builds the plain ``IterableOperator`` of ``.iterate()`` instead: one task
        instance, no mapping, so a DAG can take the count from a parameter without a branch.

        The wrapped partial's ``_expand`` builds the in-memory ``MappedOperator`` (never registered
        with the DAG), exactly as its own ``.iterate()`` does for a single task instance.

        :param expand_input: The input to iterate against.
        :param strict: Whether to enforce strict argument checking.
        """


@attrs.define(kw_only=True, repr=False)
class PartialOperatorSpread(OperatorSpread[OperatorPartial]):
    """
    The OperatorSpread of a classic (non-decorated) operator's OperatorPartial.

    ``iterate()`` and ``iterate_kwargs()`` validate their input the way the partial's own do,
    and return the ``MappedIterableOperator`` itself, as ``.expand()`` returns the MappedOperator
    (the plain ``IterableOperator`` when ``across=1``).

    :param operator_partial: The OperatorPartial to spread.
    :param across: The number of task instances to create. Items are distributed across them
        round-robin (item ``i`` goes to task instance ``i % across``), not split into ``across``
        contiguous chunks. Exactly ``across`` task instances are always created, even when the
        input yields fewer items; ``across=1`` is the single task instance of ``.iterate()``.
    """

    def iterate(self, **mapped_kwargs: OperatorExpandArgument) -> MappedIterableOperator | IterableOperator:
        # Since the input is already checked at parse time, we can set strict
        # to False to skip the checks on execution.
        return self._iterate(self.operator_partial._iterate_input(**mapped_kwargs), strict=False)

    def iterate_kwargs(
        self, kwargs: OperatorExpandKwargsArgument, *, strict: bool = True
    ) -> MappedIterableOperator | IterableOperator:
        return self._iterate(self.operator_partial._iterate_kwargs_input(kwargs), strict=strict)

    def _iterate(
        self, expand_input: ExpandInput, *, strict: bool
    ) -> MappedIterableOperator | IterableOperator:
        from airflow.sdk.definitions.iterableoperator import IterableOperator, MappedIterableOperator

        operator = self.operator_partial._expand(expand_input, strict=strict, register_with_dag=False)
        if self.across == 1:
            # One task instance: the plain IterableOperator the partial's own .iterate() builds.
            return IterableOperator(operator=operator, expand_input=expand_input)
        return MappedIterableOperator(
            mapped_operator=operator, expand_input=expand_input, spread_across=self.across
        )


@attrs.define(kw_only=True, repr=False)
class DecoratedOperatorSpread(OperatorSpread[_TaskDecorator]):
    """
    The OperatorSpread of a TaskFlow ``@task`` decorator.

    ``iterate()`` and ``iterate_kwargs()`` validate their input the way the decorator's own do,
    and return an XComArg for downstream dependencies, as the decorator's ``.expand()`` does.

    :param operator_partial: The _TaskDecorator to spread.
    :param across: The number of task instances to create. Items are distributed across them
        round-robin (item ``i`` goes to task instance ``i % across``), not split into ``across``
        contiguous chunks. Exactly ``across`` task instances are always created, even when the
        input yields fewer items; ``across=1`` is the single task instance of ``.iterate()``.
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

    def _iterate(
        self, expand_input: ExpandInput, *, strict: bool
    ) -> MappedIterableOperator | IterableOperator:
        from airflow.sdk.definitions.iterableoperator import IterableOperator, MappedIterableOperator

        operator = self.operator_partial._expand(expand_input, strict=strict, register_with_dag=False)
        if self.across == 1:
            # One task instance: the plain IterableOperator the decorator's own .iterate() builds.
            return IterableOperator(operator=operator, expand_input=DecoratedExpandInput(expand_input))
        return MappedIterableOperator(
            mapped_operator=operator,
            expand_input=DecoratedExpandInput(expand_input),
            spread_across=self.across,
        )
