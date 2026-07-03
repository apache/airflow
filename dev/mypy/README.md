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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Apache Airflow Mypy Plugins](#apache-airflow-mypy-plugins)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Available Plugins](#available-plugins)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Apache Airflow Mypy Plugins

This package provides Mypy plugins for Apache Airflow to enhance type checking capabilities.

## Installation

```bash
pip install apache-airflow-mypy
```

## Usage

Add the plugins to your `mypy.ini` or `pyproject.toml` configuration:

### Using mypy.ini

```ini
[mypy]
plugins = airflow_mypy.plugins.decorators, airflow_mypy.plugins.outputs
```

### Using pyproject.toml

```toml
[tool.mypy]
plugins = ["airflow_mypy.plugins.decorators", "airflow_mypy.plugins.outputs"]
```

## Available Plugins

### decorators Plugin

Provides type checking support for Airflow decorators that modify function signatures.

### outputs Plugin

Handles type checking for operator outputs and XComArg types, allowing proper type inference when passing task outputs between tasks.
