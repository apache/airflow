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

import pytest

from airflow.exceptions import DeserializingResultError
from airflow.utils.context import AirflowContextDeprecationWarning, suppress_and_warn


def method_which_raises_an_airflow_deprecation_warning():
    raise AirflowContextDeprecationWarning()


class TestContext:
    def test_suppress_and_warn_when_raised_exception_is_suppressed(self):
        with suppress_and_warn(AirflowContextDeprecationWarning):
            method_which_raises_an_airflow_deprecation_warning()

    def test_suppress_and_warn_when_raised_exception_is_not_suppressed(self):
        with pytest.raises(AirflowContextDeprecationWarning):
            with suppress_and_warn(DeserializingResultError):
                method_which_raises_an_airflow_deprecation_warning()
