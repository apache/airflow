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

from typing import TYPE_CHECKING, Union

from airflow.models.baseoperator import BaseOperator
from airflow.models.mappedoperator import MappedOperator

if TYPE_CHECKING:
    from airflow.models.abstractoperator import AbstractOperator
    from airflow.typing_compat import TypeGuard

Operator = Union[BaseOperator, MappedOperator]


def needs_expansion(task: AbstractOperator) -> TypeGuard[Operator]:
    """Whether a task needs expansion at runtime.

    A task needs expansion if it either

    * Is a mapped operator, or
    * Is in a mapped task group.

    This is implemented as a free function (instead of a property) so we can
    make it a type guard.
    """
    if isinstance(task, MappedOperator):
        return True
    if task.get_closest_mapped_task_group() is not None:
        return True
    return False


__all__ = ["Operator", "needs_expansion"]
