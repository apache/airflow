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

import airflow
from airflow.exceptions import AirflowException


def test_deprecated_exception():
    warning_pattern = "Import 'AirflowException' directly from the airflow module is deprecated"
    with pytest.warns(DeprecationWarning, match=warning_pattern):
        # If there is no warning, then most possible it imported somewhere else.
        assert getattr(airflow, "AirflowException") is AirflowException
