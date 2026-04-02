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

Authoring and Scheduling
=========================

Here you can find detailed documentation about advanced authoring and scheduling Airflow Dags.
It's recommended that you first review the pages in :doc:`core concepts </core-concepts/index>`

.. _authoring-section:

**Authoring**

.. toctree::
    :maxdepth: 2

    deferring
    serializers
    connections
    dynamic-task-mapping
    assets

.. _scheduling-section:

**Scheduling**

.. toctree::
    :maxdepth: 2

    cron
    timezone
    Asset-Aware Scheduling <asset-scheduling>
    timetable
    Event-Driven Scheduling <event-scheduling>
