
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


OpenLineage Airflow integration
--------------------------------------------

OpenLineage is an open framework for data lineage collection and analysis.
At its core it is an extensible specification that systems can use to interoperate with lineage metadata.
`Check out OpenLineage docs <https://openlineage.io/docs/>`_.

Quickstart
==========

To instrument your Airflow instance with OpenLineage, see :ref:`guides/user:openlineage`.

To implement OpenLineage support for Airflow Operators, see :ref:`guides/developer:openlineage`.

What's in it for me ?
=====================

The metadata collected can answer questions like:

- Why did specific data transformation fail?
- What are the upstream sources feeding into certain dataset?
- What downstream processes rely on this specific dataset?
- Is my data fresh?
- Can I identify the bottleneck in my data processing pipeline?
- How did the latest code change affect data processing times?
- How can I trace the cause of data inaccuracies in my report?
- How are data privacy and compliance requirements being managed through the data's lifecycle?
- Are there redundant data processes that can be optimized or removed?
- What data dependencies exist for this critical report?

Understanding complex inter-Dag dependencies and providing up-to-date runtime visibility into Dag execution can be challenging.
OpenLineage integrates with Airflow to collect Dag lineage metadata so that inter-Dag dependencies are easily maintained
and viewable via a lineage graph, while also keeping a catalog of historical runs of Dags.

For OpenLineage backend that will receive events, you can use `Marquez <https://marquezproject.ai/>`_

How it works under the hood ?
=============================

OpenLineage integration implements `AirflowPlugin <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html>`_.
This allows it to be discovered on Airflow start and register
`Airflow Listener <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/listeners.html>`_.

The ``OpenLineageListener`` is then called by Airflow when certain events happen - when Dags or TaskInstances start, complete or fail.
For Dags, the listener runs in Airflow Scheduler. For TaskInstances, the listener runs on Airflow Worker.

When TaskInstance listener method gets called, the ``OpenLineageListener`` constructs metadata like event's unique ``run_id`` and event time.
Then, it tries to extract metadata from Airflow Operators as described in :ref:`extraction_precedence:openlineage`.
