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

import itertools
import operator

import pytest
from pendulum import DateTime

from airflow.timefilters.base import TimeFilter

DATE = DateTime(1970, 1, 1)


class DummyTimeFilter(TimeFilter):
    def __init__(self, matches):
        self.matches = matches

    def match(self, date):
        assert date is DATE
        return self.matches


@pytest.mark.parametrize("operator", [operator.and_, operator.or_])
def test_boolean(operator):
    values = (True, False)
    for combination in itertools.product(values, values):
        timefilters = list(map(DummyTimeFilter, combination))
        assert operator(*combination) == operator(*timefilters).match(DATE)
