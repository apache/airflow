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

Versioning and compatibility
============================

Workers run tasks with the Task SDK, while the Scheduler and API server run Airflow core. These
components can be upgraded at different times, so it matters which combinations work together.
This page explains how the Task SDK is versioned and what happens at runtime when its version
differs from the version of the Airflow core it talks to.

Two kinds of version
--------------------

**Package version.** ``apache-airflow-task-sdk`` and ``apache-airflow-core`` are released as separate
distributions. Each declares the range of the other it supports in its ``pyproject.toml``. For example,
the Task SDK requires ``apache-airflow-core`` within a bounded range, and Airflow core requires
``apache-airflow-task-sdk`` within a bounded range. Installing a combination outside these ranges is
not supported, and the package manager will normally refuse to resolve it.

**Execution API version.** While a task runs, the Task SDK does not access the metadata database.
It calls the Execution API served by the API server, and every request carries an
``airflow-api-version`` header. This is a date-based (CalVer) version such as ``2027-02-28``. The
version a given Task SDK release speaks is the ``API_VERSION`` constant in
``airflow.sdk.api.datamodels._generated``.

The API server uses `Cadwyn <https://github.com/zmievsa/cadwyn>`__ to serve every released API version
at once. A change that affects existing clients, such as a new required field or a changed response,
becomes a new API version, and the server keeps a migration that converts responses back to the
older shapes. See the `contributor guide to Execution API versioning
<https://github.com/apache/airflow/blob/main/contributing-docs/19_execution_api_versioning.rst>`__
for how these versions are authored.

Runtime behavior
----------------

Older Task SDK with a newer API server
    This is the supported direction for rolling upgrades. The server answers a request with the
    shape of the version the Task SDK asked for, so the task behaves as it did before the server was
    upgraded.

Newer Task SDK with an older API server
    This is not supported. The server serves a request for a version it does not know using the closest
    earlier version it does know. The Task SDK may then send requests to endpoints, or expect fields,
    that the older server does not have, and the task can fail at runtime.

Upgrade order
-------------

Because older Task SDK releases keep working against newer servers, upgrade the components that run Airflow
core (API server, Scheduler, Dag processor, Triggerer) before upgrading the workers that run the
Task SDK. Keep every component within the package version ranges described above.

.. note::

   The Dag authoring interface in ``airflow.sdk`` is a separate matter from the Execution API version.
   Dag files import from ``airflow.sdk`` and are written against the Task SDK release installed
   where they are parsed and run. See :doc:`index` for the authoring interface.
