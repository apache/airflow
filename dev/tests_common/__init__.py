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

import os
from pathlib import Path

# This constant is set to True if tests are run with Airflow installed from Packages rather than running
# the tests within Airflow sources. While most tests in CI are run using Airflow sources, there are
# also compatibility tests that only use `tests` package and run against installed packages of Airflow in
# for supported Airflow versions.
RUNNING_TESTS_AGAINST_AIRFLOW_PACKAGES = (
    "USE_AIRFLOW_VERSION" in os.environ
    or not (Path(__file__).parents[2] / "airflow" / "__init__.py").exists()
)
