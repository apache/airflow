# SeaTunnel Provider Integration Tests

This directory contains integration tests for the Apache Airflow SeaTunnel provider. These tests verify the functionality of the provider components against a real SeaTunnel instance.

## Overview

The integration tests are designed to:

- Test the SeaTunnel provider components (hooks, operators, sensors) against a real SeaTunnel instance
- Verify end-to-end workflows
- Ensure compatibility with SeaTunnel 2.3.11
- Validate configuration and error handling

## Requirements

- **Apache Airflow**: 3.0.3+ (tested and recommended)
- **Python**: 3.12 (recommended), 3.8-3.11 (compatible)
- **Docker**: 20.10+ with Docker Compose 2.0+
- **SeaTunnel**: 2.3.11 (via Docker)
- **Operating Systems**: Linux, macOS, Windows (WSL)

## Test Structure

The test suite follows the Apache Airflow provider testing conventions, similar to the Kafka provider:

```
tests/integration/
├── __init__.py
├── conftest.py                    # Pytest configuration and fixtures
├── docker-compose.yml            # SeaTunnel service setup
├── pytest.ini                    # Pytest configuration
├── requirements.txt               # Test dependencies
├── run_tests.sh                   # Test runner script
├── README.md                      # This file
├── test_configs/                  # Test configuration files
│   ├── simple_batch_job.conf
│   └── streaming_job.conf
├── hooks/
│   ├── __init__.py
│   └── test_seatunnel_hook.py     # Hook integration tests
├── operators/
│   ├── __init__.py
│   └── test_seatunnel_operator.py # Operator integration tests
├── sensors/
│   ├── __init__.py
│   └── test_seatunnel_sensor.py   # Sensor integration tests
└── test_seatunnel_integration.py  # End-to-end integration tests
```

## Prerequisites

Before running the integration tests, ensure you have:

1. **Python Environment**:
   - Python 3.12 (recommended)
   - Apache Airflow 3.0.3 (tested and recommended)
   - Minimum: Python 3.8+ with Airflow 2.3.0+

2. **Docker Environment**:
   - Docker 20.10+ 
   - Docker Compose 2.0+
   - Available ports: 8080, 8081, 8083

3. **System Requirements**:
   - Linux, macOS, or Windows (WSL)
   - At least 4GB RAM available for Docker containers

## Running the Tests

### Quick Start

The easiest way to run the integration tests is using the provided script:

```bash
cd tests/integration
./run_tests.sh
```

This will:
1. Install test dependencies
2. Start SeaTunnel services using Docker Compose
3. Run all integration tests
4. Clean up services after completion

### Advanced Usage

#### Run with verbose output:
```bash
./run_tests.sh -v
```

#### Skip Docker setup (use existing services):
```bash
./run_tests.sh -s
```

#### Run specific test patterns:
```bash
./run_tests.sh -p "test_hook*"
./run_tests.sh -p "test_*operator*"
```

#### Use a different SeaTunnel version:
```bash
./run_tests.sh --seatunnel-version 2.3.10
```

### Manual Test Execution

If you prefer to run tests manually:

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   pip install -e ../../  # Install provider in development mode
   ```

2. **Start SeaTunnel services:**
   ```bash
   docker-compose up -d
   ```

3. **Wait for services to be ready:**
   ```bash
   # Wait for SeaTunnel to start (usually takes 30-60 seconds)
   curl -f http://localhost:8083/health
   ```

4. **Run tests:**
   ```bash
   pytest -v -m integration
   ```

5. **Clean up:**
   ```bash
   docker-compose down -v
   ```

## Test Categories

### Integration Tests (`@pytest.mark.integration`)
Tests that require external services (SeaTunnel) to be running.

### Slow Tests (`@pytest.mark.slow`)
Tests that take longer to execute (e.g., end-to-end workflows).

### Docker Tests (`@pytest.mark.requires_docker`)
Tests that specifically require Docker to be running.

## Test Configuration

### Environment Variables

- `SKIP_INTEGRATION_TESTS=true`: Skip all integration tests
- `SKIP_DOCKER_SETUP=true`: Skip Docker service setup
- `SEATUNNEL_VERSION`: SeaTunnel version to use (default: 2.3.11)

### SeaTunnel Configuration

The tests use a SeaTunnel instance configured with:
- **Zeta Engine**: Primary engine for testing
- **REST API**: Enabled on port 8083 for job monitoring
- **Web UI**: Available on port 8080
- **Test Configurations**: Located in `test_configs/`

## Test Data and Configurations

### Batch Job Configuration (`simple_batch_job.conf`)
A simple batch job that:
- Uses FakeSource to generate test data
- Applies basic transformations
- Outputs to Console sink

### Streaming Job Configuration (`streaming_job.conf`)
A streaming job that:
- Uses FakeSource with streaming mode
- Demonstrates checkpoint configuration
- Outputs to Console sink

## Troubleshooting

### Common Issues

1. **Docker not running:**
   ```
   Error: Docker is not running
   ```
   **Solution:** Start Docker Desktop or Docker daemon

2. **Port conflicts:**
   ```
   Error: Port 8083 is already in use
   ```
   **Solution:** Stop other services using these ports or modify docker-compose.yml

3. **SeaTunnel startup timeout:**
   ```
   Error: SeaTunnel failed to start within timeout
   ```
   **Solution:** Increase timeout or check Docker logs with `docker-compose logs`

4. **Test failures due to timing:**
   ```
   Error: Connection refused
   ```
   **Solution:** Ensure SeaTunnel is fully started before running tests

### Debugging

1. **Check SeaTunnel logs:**
   ```bash
   docker-compose logs seatunnel
   ```

2. **Check SeaTunnel health:**
   ```bash
   curl http://localhost:8083/health
   ```

3. **Run tests with more verbose output:**
   ```bash
   pytest -v -s -m integration
   ```

4. **Run a single test:**
   ```bash
   pytest -v tests/integration/hooks/test_seatunnel_hook.py::TestSeaTunnelHookIntegration::test_hook_initialization
   ```

## Contributing

When adding new integration tests:

1. Follow the existing test structure and naming conventions
2. Use appropriate pytest markers (`@pytest.mark.integration`, etc.)
3. Ensure tests clean up after themselves
4. Add documentation for new test configurations
5. Update this README if adding new test categories or requirements

## SeaTunnel Version Compatibility

These tests are designed for SeaTunnel 2.3.11, but should work with other 2.3.x versions. Key compatibility considerations:

- **REST API**: Available in Zeta engine (2.3.0+)
- **Configuration Format**: HOCON format used in 2.x versions
- **Engine Support**: Zeta, Flink, and Spark engines

For other versions, you may need to:
- Update the Docker image version in `docker-compose.yml`
- Modify test configurations for API changes
- Adjust timeout values for different startup times