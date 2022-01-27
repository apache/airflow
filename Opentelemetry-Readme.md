.. Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

.. http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.

Documentation
#############

This contains documentation for implementing Opentelemetry with Apache Airflow project

## Running Opentelemetry in development

First clone the airflow repository. See 'Airflow-GitHub <https://github.com/apache/airflow>'

Once you have airflow set-up locally, you can simply get opentelemetry traces by adding the '--integration jaeger' flag

.. code-block:: bash

    ./breeze start-airflow --integration jaeger

Then, view your traces at `localhost:16686`
