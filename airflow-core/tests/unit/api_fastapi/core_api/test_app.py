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

from tests_common.test_utils.db import clear_db_jobs

pytestmark = pytest.mark.db_test


class TestGzipMiddleware:
    @pytest.fixture(autouse=True)
    def setup(self):
        clear_db_jobs()
        yield
        clear_db_jobs()

    def test_gzip_middleware_should_not_be_chunked(self, test_client) -> None:
        response = test_client.get("/api/v2/monitor/health")
        headers = {k.lower(): v for k, v in response.headers.items()}

        # Ensure we do not reintroduce Transfer-Encoding: chunked
        assert "transfer-encoding" not in headers
