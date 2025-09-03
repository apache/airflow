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

from abc import abstractmethod
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing import TypeAlias

    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.edges import EdgeModifier
    from airflow.sdk.definitions.mappedoperator import MappedOperator

    Operator: TypeAlias = BaseOperator | MappedOperator

# TODO: Should this all just live on DAGNode?


class DependencyMixin:
    """Mixing implementing common dependency setting methods like >> and <<."""

    @property
    def roots(self) -> Iterable[DependencyMixin]:
        """
        List of root nodes -- ones with no upstream dependencies.

        a.k.a. the "start" of this sub-graph
        """
        raise NotImplementedError()

    @property
    def leaves(self) -> Iterable[DependencyMixin]:
        """
        List of leaf nodes -- ones with only upstream dependencies.

        a.k.a. the "end" of this sub-graph
        """
        raise NotImplementedError()

    @abstractmethod
    def set_upstream(
        self, other: DependencyMixin | Sequence[DependencyMixin], edge_modifier: EdgeModifier | None = None
    ):
        """Set a task or a task list to be directly upstream from the current task."""
        raise NotImplementedError()

    @abstractmethod
    def set_downstream(
        self, other: DependencyMixin | Sequence[DependencyMixin], edge_modifier: EdgeModifier | None = None
    ):
        """Set a task or a task list to be directly downstream from the current task."""
        raise NotImplementedError()

    def as_setup(self) -> DependencyMixin:
        """Mark a task as setup task."""
        raise NotImplementedError()

    def as_teardown(
        self,
        *,
        setups: BaseOperator | Iterable[BaseOperator] | None = None,
        on_failure_fail_dagrun: bool | None = None,
    ) -> DependencyMixin:
        """Mark a task as teardown and set its setups as direct relatives."""
        raise NotImplementedError()

    def update_relative(
        self, other: DependencyMixin, upstream: bool = True, edge_modifier: EdgeModifier | None = None
    ) -> None:
        """
        Update relationship information about another TaskMixin. Default is no-op.

        Override if necessary.
        """

    def __lshift__(self, other: DependencyMixin | Sequence[DependencyMixin]):
        """Implement Task << Task."""
        self.set_upstream(other)
        return other

    def __rshift__(self, other: DependencyMixin | Sequence[DependencyMixin]):
        """Implement Task >> Task."""
        self.set_downstream(other)
        return other

    def __rrshift__(self, other: DependencyMixin | Sequence[DependencyMixin]):
        """Implement Task >> [Task] because list don't have __rshift__ operators."""
        self.__lshift__(other)
        return self

    def __rlshift__(self, other: DependencyMixin | Sequence[DependencyMixin]):
        """Implement Task << [Task] because list don't have __lshift__ operators."""
        self.__rshift__(other)
        return self

    @classmethod
    def _iter_references(cls, obj: Any) -> Iterable[tuple[DependencyMixin, str]]:
        from airflow.sdk.definitions._internal.abstractoperator import AbstractOperator

        if isinstance(obj, AbstractOperator):
            yield obj, "operator"
        elif isinstance(obj, ResolveMixin):
            yield from obj.iter_references()
        elif isinstance(obj, Sequence):
            for o in obj:
                yield from cls._iter_references(o)


class ResolveMixin:
    """A runtime-resolved value."""

    def iter_references(self) -> Iterable[tuple[Operator, str]]:
        """
        Find underlying XCom references this contains.

        This is used by the Dag parser to recursively find task dependencies.

        :meta private:
        """
        raise NotImplementedError

    def resolve(self, context: Context) -> Any:
        """
        Resolve this value for runtime.

        :meta private:
        """
        raise NotImplementedError
