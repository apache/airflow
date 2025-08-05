"""
Pytest configuration for SeaTunnel provider integration tests.
"""
import os
import pytest
import subprocess
import time
import requests
from pathlib import Path


@pytest.fixture(scope="session")
def docker_compose_file():
    """Path to the docker-compose file for integration tests."""
    return Path(__file__).parent / "docker-compose.yml"


@pytest.fixture(scope="session")
def seatunnel_service(docker_compose_file):
    """
    Start SeaTunnel service using docker-compose for integration tests.
    This fixture ensures SeaTunnel is running before tests and cleans up after.
    """
    # Check if we should skip integration tests
    if os.getenv("SKIP_INTEGRATION_TESTS", "false").lower() == "true":
        pytest.skip("Integration tests are disabled")
    
    # Start the service
    subprocess.run(
        ["docker-compose", "-f", str(docker_compose_file), "up", "-d"],
        check=True,
        cwd=docker_compose_file.parent
    )
    
    # Wait for service to be ready
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = requests.get("http://localhost:8083/status", timeout=5)
            if response.status_code == 200 and response.text.strip() == "OK":
                break
        except (requests.RequestException, requests.ConnectionError):
            pass
        
        time.sleep(2)
        retry_count += 1
    
    if retry_count >= max_retries:
        # Clean up on failure
        subprocess.run(
            ["docker-compose", "-f", str(docker_compose_file), "down"],
            cwd=docker_compose_file.parent
        )
        pytest.fail("SeaTunnel service failed to start within timeout")
    
    yield
    
    # Cleanup
    subprocess.run(
        ["docker-compose", "-f", str(docker_compose_file), "down"],
        cwd=docker_compose_file.parent
    )


@pytest.fixture
def seatunnel_config_dir():
    """Path to test configuration files."""
    return Path(__file__).parent / "test_configs"


@pytest.fixture
def simple_batch_config(seatunnel_config_dir):
    """Path to simple batch job configuration."""
    return seatunnel_config_dir / "simple_batch_job.conf"


@pytest.fixture
def streaming_config(seatunnel_config_dir):
    """Path to streaming job configuration."""
    return seatunnel_config_dir / "streaming_job.conf"


@pytest.fixture
def seatunnel_connection_params():
    """Standard connection parameters for SeaTunnel tests."""
    return {
        "conn_id": "seatunnel_test",
        "conn_type": "seatunnel",
        "host": "localhost",
        "port": 8083,
        "extra": '{"seatunnel_home": "/opt/seatunnel"}'
    }


# Markers for different test categories
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "requires_docker: mark test as requiring Docker"
    )