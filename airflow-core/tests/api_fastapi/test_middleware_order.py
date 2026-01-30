import pytest


@pytest.mark.integration
def test_health_endpoint_not_chunked(test_client):
    response = test_client.get("/api/v2/monitor/health")

    # Ensure we do not reintroduce Transfer-Encoding: chunked
    assert "transfer-encoding" not in {
        k.lower(): v for k, v in response.headers.items()
    }
