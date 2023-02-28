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

Auth backends
-------------

This is a summary of all Apache Airflow Community provided implementations of authentication backends
exposed via community-managed providers.

Airflow's authentication for web server and API is based on Flask Application Builder's authentication
capabilities. You can read more about those in
`FAB security docs <https://flask-appbuilder.readthedocs.io/en/latest/security.html>`_.

You can also
take a look at Auth backends available in the core Airflow in :doc:`apache-airflow:administration-and-deployment/security/webserver`
or see those provided by the community-managed providers:

.. airflow-auth-backends::
   :tags: None
   :header-separator: "
