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

from airflow import settings

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def async_engine_without_pool():
    """
    Give the test an unpooled async engine.

    For tests that send write requests through the test client and also call ``dag.test()``: the two
    run on different event loops, and a pooled async connection only works on the loop that opened it.
    """
    previous = settings.async_engine, settings.AsyncSession
    with conf_vars({("database", "sql_alchemy_pool_enabled"): "False"}):
        settings._configure_async_session()
    yield
    settings.async_engine, settings.AsyncSession = previous
