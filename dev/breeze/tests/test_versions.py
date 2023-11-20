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

from airflow_breeze.utils.versions import strip_leading_zeros_from_version


@pytest.mark.parametrize(
    "version,stripped_version", [("3.4.0", "3.4.0"), ("13.04.05", "13.4.5"), ("0003.00004.000005", "3.4.5")]
)
def test_strip_leading_versions(version: str, stripped_version):
    assert stripped_version == strip_leading_zeros_from_version(version)
