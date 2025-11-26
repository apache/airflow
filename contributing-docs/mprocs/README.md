<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# mprocs Configuration for Airflow

This directory contains mprocs configuration files for running Airflow components.

## Overview

mprocs is a modern alternative to tmux for managing multiple processes. It provides:

- Intuitive TUI with better visual feedback
- Easy process management (start, stop, restart)
- Mouse and keyboard navigation
- Process status indicators
- Better cross-platform support

## Files

- `basic.yaml` - Static mprocs configuration example
- `generate_mprocs_config.py` - Script to generate dynamic mprocs configuration (located in `scripts/in_container/bin/`)
- `run_mprocs` - Entry point script for starting Airflow with mprocs (located in `scripts/in_container/bin/`)

## Usage

### Using Breeze

The easiest way to use mprocs with Airflow is through the Breeze command:

```bash
breeze start-airflow --use-mprocs
```

This will:

1. Start the Breeze container
2. Dynamically generate an mprocs configuration based on your environment
3. Launch mprocs with all configured Airflow components

### With Debug Support

You can combine mprocs with debugging:

```bash
breeze start-airflow --use-mprocs --debug scheduler --debug triggerer
```

### Manual Usage

Inside the Breeze container, you can manually generate and use mprocs configuration:

```bash
# Generate configuration
python3 /opt/airflow/scripts/in_container/bin/generate_mprocs_config.py /tmp/airflow-mprocs.yaml

# Start mprocs
mprocs --config /tmp/airflow-mprocs.yaml
```

## Dynamic Configuration

The `generate_mprocs_config.py` script reads environment variables to determine which components to run:

- `INTEGRATION_CELERY` - Enables Celery worker
- `CELERY_FLOWER` - Enables Flower UI
- `STANDALONE_DAG_PROCESSOR` - Enables standalone DAG processor
- `AIRFLOW__CORE__EXECUTOR` - Determines if Edge worker should run
- `USE_AIRFLOW_VERSION` - Determines if API server (3.x+) or webserver (2.x) should run
- `BREEZE_DEBUG_*` - Enables debug mode for specific components
- `DEV_MODE` - Enables development mode features

## Components

The following Airflow components can be managed by mprocs:

1. **scheduler** - Main Airflow scheduler
2. **api_server** (Airflow 3.x+) or **webserver** (Airflow 2.x)
3. **triggerer** - Handles deferred tasks
4. **dag_processor** - Standalone Dag processor (optional)
5. **celery_worker** - Celery worker (when using CeleryExecutor)
6. **flower** - Celery Flower monitoring UI (optional)
7. **edge_worker** - Edge worker (when using EdgeExecutor)

## mprocs Controls

When mprocs is running:

- **Arrow keys** - Navigate between processes
- **Space** - Show/hide process output
- **r** - Restart selected process
- **k** - Kill selected process
- **s** - Start selected process
- **a** - Toggle showing all processes
- **q** - Quit mprocs
- **?** - Show help

## Requirements

mprocs is pre-installed in the Breeze CI image starting from Airflow 3.x development. No additional setup is required.

For older images or custom setups, you can manually install mprocs by adding it to your Dockerfile or by customizing your Breeze image. See the installation section in the Dockerfile.ci for reference.

## See Also

- [Debugging Airflow Components](../../contributing-docs/20_debugging_airflow_components.rst)
- [Contributors Quick Start](../../contributing-docs/03_contributors_quick_start.rst)
- [mprocs Quick Reference](../../contributing-docs/mprocs/MPROCS_QUICK_REFERENCE.md)
