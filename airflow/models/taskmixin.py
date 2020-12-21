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

from abc import abstractmethod
from typing import Sequence, Union


class TaskMixin:
    """
    Mixing implementing common chain methods like >> and <<.

    In the following functions we use:
    Task = Union[BaseOperator, XComArg]
    No type annotations due to cyclic imports.
    """

    @property
    def roots(self):
        """Should return list of root operator List[BaseOperator]"""
        raise NotImplementedError()

    @property
    def leaves(self):
        """Should return list of leaf operator List[BaseOperator]"""
        raise NotImplementedError()

    @abstractmethod
    def set_upstream(self, other: Union["TaskMixin", Sequence["TaskMixin"]]):
        """Set a task or a task list to be directly upstream from the current task."""
        raise NotImplementedError()

    @abstractmethod
    def set_downstream(self, other: Union["TaskMixin", Sequence["TaskMixin"]]):
        """Set a task or a task list to be directly downstream from the current task."""
        raise NotImplementedError()

    def update_relative(self, other: "TaskMixin", upstream=True) -> None:
        """
        Update relationship information about another TaskMixin. Default is no-op.
        Override if necessary.
        """

    def __lshift__(self, other: Union["TaskMixin", Sequence["TaskMixin"]]):
        """Implements Task << Task"""
        self.set_upstream(other)
        return other

    def __rshift__(self, other: Union["TaskMixin", Sequence["TaskMixin"]]):
        """Implements Task >> Task"""
        self.set_downstream(other)
        return other

    def __rrshift__(self, other: Union["TaskMixin", Sequence["TaskMixin"]]):
        """Called for Task >> [Task] because list don't have __rshift__ operators."""
        self.__lshift__(other)
        return self

    def __rlshift__(self, other: Union["TaskMixin", Sequence["TaskMixin"]]):
        """Called for Task << [Task] because list don't have __lshift__ operators."""
        self.__rshift__(other)
        return self
