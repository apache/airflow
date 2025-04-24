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

# airflowctl

A command-line tool for interacting with Apache Airflow instances through the Airflow REST API. It offers a convenient interface for performing common operations remotely without direct access to the Airflow scheduler or webserver.

## Features

- Communicates with Airflow instances through the REST API
- Supports authentication using Airflow API tokens
- Executes commands against remote Airflow deployments
- Provides intuitive command organization with group-based structure
- Includes detailed help documentation for all commands

## Requirements

- Python 3.9 or later (compatible with Python >= 3.9 and < 3.13)
- Network access to an Apache Airflow instance with REST API enabled

## Usage

Access the tool from your terminal:

```bash
airflowctl --help
```

## Contributing

Want to help improve Apache Airflow? Check out our [contributing documentation](https://github.com/apache/airflow/blob/main/contributing-docs/README.rst).
