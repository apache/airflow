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

.. _concepts:message-queues:

Message Queues
==============

The Message Queues are a way to expose capability of external event-driven scheduling of Dags.

Apache Airflow is primarily designed for time-based and dependency-based scheduling of workflows. However,
modern data architectures often require near real-time processing and the ability to react to
events from various sources, such as message queues.

Airflow has native event-driven capability, allowing users to create workflows that can be
triggered by external events, thus enabling more responsive data pipelines.

Airflow supports poll-based event-driven scheduling, where the Triggerer can poll
external message queues using built-in :class:`airflow.triggers.base.BaseTrigger` classes. This allows users
to create workflows that can be triggered by external events, such as messages arriving
in a queue or changes in a database efficiently.

Airflow constantly monitors the state of an external resource and updates the asset whenever the external
resource reaches a given state (if it does reach it). To achieve this, we leverage Airflow Triggers.
Triggers are small, asynchronous pieces of Python code whose job is to poll an external resource state.

The list of supported message queues is available in :doc:`apache-airflow-providers:core-extensions/message-queues`.
