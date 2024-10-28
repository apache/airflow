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

from airflow_breeze.utils.packages import get_long_package_names


@pytest.mark.parametrize(
    "short_form_providers, expected",
    [
        pytest.param(
            ("awesome", "foo.bar"),
            ("apache-airflow-providers-awesome", "apache-airflow-providers-foo-bar"),
            id="providers",
        ),
        pytest.param(
            ("apache-airflow", "helm-chart", "docker-stack"),
            ("apache-airflow", "helm-chart", "docker-stack"),
            id="non-providers-docs",
        ),
        pytest.param(
            ("apache-airflow-providers",),
            ("apache-airflow-providers",),
            id="providers-index",
        ),
        pytest.param(
            ("docker", "docker-stack", "apache-airflow-providers"),
            (
                "apache-airflow-providers-docker",
                "docker-stack",
                "apache-airflow-providers",
            ),
            id="mixin",
        ),
    ],
)
def test_get_provider_name_from_short_hand(short_form_providers, expected):
    assert get_long_package_names(short_form_providers) == expected
