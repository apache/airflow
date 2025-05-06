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

Connections
-----------

This is a summary of all Apache Airflow Community provided implementations of connections
exposed via community-managed providers.

Airflow can be extended by providers with custom connections. Each provider can define their own custom
connections, that can define their own custom parameters and UI customizations/field behaviours for each
connection, when the connection is managed via Airflow UI. Those connections also define connection types,
that can be used to automatically create Airflow Hooks for specific connection types.

The connection management is explained in
:doc:`apache-airflow:authoring-and-scheduling/connections` and you can also see those
provided by the community-managed providers:

.. airflow-connections::
   :tags: None
   :header-separator: "
