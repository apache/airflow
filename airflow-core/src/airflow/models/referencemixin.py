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

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Iterable
    from typing import TypeAlias

    from airflow.models.mappedoperator import MappedOperator
    from airflow.serialization.serialized_objects import SerializedBaseOperator

    Operator: TypeAlias = MappedOperator | SerializedBaseOperator


@runtime_checkable
class ReferenceMixin(Protocol):
    """
    Mixin for things that references a task.

    This should be implemented by things that reference operators and use them
    to lazily resolve values at runtime. The most prominent examples are XCom
    references (XComArg).

    This is a partial interface to the SDK's ResolveMixin with the resolve()
    method removed since the scheduler should not need to resolve the reference.
    """

    def iter_references(self) -> Iterable[tuple[Operator, str]]:
        """
        Find underlying XCom references this contains.

        This is used by the DAG parser to recursively find task dependencies.

        :meta private:
        """
        raise NotImplementedError
