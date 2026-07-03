.. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

apache-airflow-mypy 0.1.0
-------------------------------

Initial release of apache-airflow-mypy, a package providing Mypy plugins for Apache Airflow.

Features
^^^^^^^^

- Mypy plugin for typed decorators that modify function signatures
- Mypy plugin for operator outputs and XComArg type handling
- Support for type checking Airflow decorators with injected parameters
- Proper type inference when passing task outputs between tasks

This package makes the Airflow Mypy plugins installable and reusable for users who want to
enhance type checking in their Airflow DAGs.
