from __future__ import annotations

import pytest

from tests_common.test_utils.db import clear_db_jobs

pytestmark = pytest.mark.db_test


class TestGzipMiddleware:
    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_jobs()
        yield
        clear_db_jobs()

    def test_gzip_middleware_should_not_be_chunked(self, test_client) -> None:
        response = test_client.get("/api/v2/monitor/health")
        headers = {k.lower(): v for k, v in response.headers.items()}

        # Ensure we do not reintroduce Transfer-Encoding: chunked
        assert "transfer-encoding" not in headers
