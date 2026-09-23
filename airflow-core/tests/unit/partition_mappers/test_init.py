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

import airflow.partition_mappers
import airflow.sdk


def test_package_root_exports_match_sdk():
    """Every partition-mapper symbol exported from airflow.sdk must also be exported from the core package root."""
    sdk_names = {
        name
        for name, module in airflow.sdk.__lazy_imports.items()
        if module.startswith(".definitions.partition_mappers")
    }
    assert sdk_names == set(airflow.partition_mappers.__all__)
