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
from typing import NamedTuple

from pendulum import DateTime

from airflow.typing_compat import Protocol


class AbstractTimeFilter(Protocol):
    """Protocol that all time filters are expected to implement."""

    @abstractmethod
    def match(self, date: DateTime) -> bool:
        """Return true if the provided date matches the condition."""


class TimeFilter(AbstractTimeFilter):
    """Base class that all built-in time filters inherit from."""

    def __eq__(self, other: AbstractTimeFilter) -> bool:
        return type(self) is type(other) and self.__dict__ == other.__dict__

    def __and__(self, other: AbstractTimeFilter) -> "TimeFilter":
        return AllTimeFilter(self, other)

    def __or__(self, other: AbstractTimeFilter) -> "TimeFilter":
        return AnyTimeFilter(self, other)


class OperatorTimeFilter(TimeFilter):
    """Time filter class used internally to combine other filters."""

    def __init__(self, *timefilters):
        self.timefilters = timefilters

    def match(self, date: DateTime) -> bool:
        class Wrapper(NamedTuple):
            timefilter: TimeFilter

            def __bool__(self):
                return self.timefilter.match(date)

        return self.operator(map(Wrapper, self.timefilters))


class AllTimeFilter(OperatorTimeFilter):
    """Matches when all filters match."""

    operator = staticmethod(all)


class AnyTimeFilter(OperatorTimeFilter):
    """Matches when any one filter matches."""

    operator = staticmethod(any)
