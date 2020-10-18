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

Logging & Monitoring
====================

Since data pipelines are generally run without any manual supervision, observability is critical.

Airflow has support for multiple logging mechanisms, as well as a built-in mechanism to emit metrics for gathering, processing, and visualization in other downstream systems. The logging capabilities are critical for diagnosis of problems which may occur in the process of running data pipelines.

In addition to the standard logging and metrics capabilities, Airflow supports the ability to detect errors in the operation of Airflow itself, using an Airflow health check. Since Airflow is generally used for running data pipelines in production, it also supports real-time error notification via integration with Sentry.

.. toctree::
    :maxdepth: 1
    :glob:

    logging-architecture
    logging-tasks
    metrics

    check-health
    errors

    tracking-user-activity
