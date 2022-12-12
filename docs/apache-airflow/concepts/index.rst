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

Concepts
========

Here you can find detailed documentation about each one of Airflow's core concepts and how to use them, as well as a high-level :doc:`architectural overview <overview>`.

**Architecture**

.. toctree::
    :maxdepth: 2

    overview


**Workloads**

.. toctree::
    :maxdepth: 2

    dags
    tasks
    operators
    dynamic-task-mapping
    sensors
    datasets
    deferring
    taskflow
    ../executor/index
    scheduler
    dagfile-processing
    pools
    timetable
    priority-weight
    cluster-policies
    serializers

**Communication**

.. toctree::
    :maxdepth: 2

    xcoms
    variables
    connections
    params
