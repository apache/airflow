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

Secret backends
---------------

This is a summary of all Apache Airflow Community provided implementations of secret backends
exposed via community-managed providers.

Airflow has the capability of reading connections, variables and configuration from Secret Backends rather
than from its own Database. While storing such information in Airflow's database is possible, many of the
enterprise customers already have some secret managers storing secrets, and Airflow can tap into those
via providers that implement secrets backends for services Airflow integrates with.

You can also take a
look at Secret backends available in the core Airflow in
:doc:`apache-airflow:administration-and-deployment/security/secrets/secrets-backend/index` and here you can see the ones
provided by the community-managed providers:

.. airflow-secrets-backends::
   :tags: None
   :header-separator: "
