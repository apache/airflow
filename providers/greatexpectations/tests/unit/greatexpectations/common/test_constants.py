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

from airflow.providers.greatexpectations.common.constants import USER_AGENT_STR, VERSION


def test_version():
    """Test that VERSION is defined."""
    assert VERSION == "1.0.0"


def test_user_agent_str():
    """Test that USER_AGENT_STR contains VERSION."""
    assert "Apache Airflow GX Operator" in USER_AGENT_STR
    assert VERSION in USER_AGENT_STR
