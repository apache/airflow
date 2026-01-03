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


def skip_cli_test(package_name: str):
    """Skip CLI tests if given package is not installed in Airflow 3.2+, as ProviderManger will try to load 'cli' section in CLI parser stage."""
    from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

    if AIRFLOW_V_3_2_PLUS:
        import importlib

        return not importlib.util.find_spec(package_name)
    return False


def skip_cli_test_marker(package_name: str, provider_name: str):
    return pytest.mark.skipif(
        skip_cli_test(package_name),
        reason=f"Skip {provider_name} CLI tests if {package_name} package is not installed in Airflow 3.2+.",
    )
