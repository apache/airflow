
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

**No change to user Dag files is required to use OpenLineage**, only basic configuration is needed so that OpenLineage knows where to send events.

.. important::

    All possible OpenLineage configuration options, with example values, can be found in :ref:`the configuration section <configuration:openlineage>`.

Quickstart
==========

.. note::

    OpenLineage offers a diverse range of data transport options (http, kafka, file etc.),
    including the flexibility to create a custom solution. Configuration can be managed through several approaches
    and there is an extensive array of settings available for users to fine-tune and enhance their use of OpenLineage.
    For a comprehensive explanation of these features, please refer to the subsequent sections of this documentation.

This example is a basic demonstration of OpenLineage user setup.
For development OpenLineage backend that will receive events, you can use `Marquez <https://marquezproject.ai/>`_

1. Install provider package or add it to ``requirements.txt`` file.

   .. code-block:: ini

      pip install apache-airflow-providers-openlineage

2. Provide a ``Transport`` configuration so that OpenLineage knows where to send the events. Within ``airflow.cfg`` file

   .. code-block:: ini

      [openlineage]
      transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}

   or with ``AIRFLOW__OPENLINEAGE__TRANSPORT`` environment variable

   .. code-block:: ini

      AIRFLOW__OPENLINEAGE__TRANSPORT='{"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}'

3. **That's it !**  When Dags are run, the integration will automatically:

  - Collect task input / output metadata (source, schema, etc.).
  - Collect task run-level metadata (execution time, state, parameters, etc.)
  - Collect task job-level metadata (owners, type, description, etc.)
  - Collect task-specific metadata (bigquery job id, python source code, etc.) - depending on the Operator

  All this data will be sent as `OpenLineage events <https://openlineage.io/docs/spec/object-model#event-payload-structure>`_ to the configured backend.

Next steps
===========
See :ref:`the configuration page <configuration:openlineage>` for more details on how to fine-tune OpenLineage to your needs.

See :ref:`guides/developer:openlineage` for details on how to add OpenLineage functionality to your Operator.

See :ref:`howto/macros:openlineage` for available macros and details on how OpenLineage defines job hierarchy.


Benefits of Data Lineage
=========================

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

How it works under the hood ?
=============================

OpenLineage integration implements `AirflowPlugin <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html>`_.
This allows it to be discovered on Airflow start and register
`Airflow Listener <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/listeners.html>`_.

The ``OpenLineageListener`` is then called by Airflow when certain events happen - when Dags or TaskInstances start, complete or fail.
For Dags, the listener runs in Airflow Scheduler. For TaskInstances, the listener runs on Airflow Worker.

When TaskInstance listener method gets called, the ``OpenLineageListener`` constructs metadata like event's unique ``run_id`` and event time.
Then, it tries to extract metadata from Airflow Operators as described in :ref:`extraction_precedence:openlineage`.

.. _client_v_provider:openlineage:

OpenLineage provider vs client
==============================

The OpenLineage integration consists of two separate packages that work together:

- **``apache-airflow-providers-openlineage``** (OpenLineage Airflow provider, this package) - Serves as the
  Airflow integration layer for OpenLineage. It extracts metadata from Airflow tasks and DAGs, implements
  the Airflow listener hooks, provides extractors for various operators, and passes the extracted metadata
  to the OpenLineage client for transmission. Keep the provider at the latest available version supported
  by your Airflow version to ensure accurate and complete lineage capture.

- **``openlineage-python``** (OpenLineage client) - Responsible for sending lineage metadata
  from Airflow to the OpenLineage backend. It handles transport configuration, event serialization, and
  communication with the backend. The client can be safely upgraded independently of Airflow and the provider
  versions to take advantage of the latest fixes, performance improvements, and features.

The provider extracts Airflow-specific metadata and formats it into OpenLineage events, while the client
handles the actual transmission of those events to your OpenLineage backend.

Provider upgrade policy
^^^^^^^^^^^^^^^^^^^^^^^^

The OpenLineage Airflow provider follows the
`Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#upgrading-minimum-supported-version-of-airflow>`_,
which means that each provider version has a minimum supported Airflow version requirement. The minimum Airflow version
is typically increased to the next MINOR release when 12 months have passed since the first release of that MINOR
version. This means that newer provider versions may require newer Airflow versions.

**Important:** The OpenLineage client (``openlineage-python``) can be upgraded independently of your
Airflow version. Unlike the provider, the client has no Airflow version dependencies, so you should
always upgrade it to the latest version to get the latest features and bug fixes.

Latest OpenLineage provider versions by Airflow version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following table shows the latest compatible OpenLineage provider version for each supported Airflow
version. Always use the latest provider version available for your Airflow version to get the most recent
bug fixes and features.

.. note::

   For the most up-to-date compatibility information, always check the provider's requirements section
   in the `official documentation <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/>`_.


.. list-table:: Latest OpenLineage Provider Versions by Airflow Version
   :widths: 30 40
   :header-rows: 1

   * - Airflow Version
     - Latest Compatible OpenLineage Provider Version
   * - 2.11.0+
     - Use latest available
   * - 2.10.x
     - 2.8.0
   * - 2.9.x
     - 2.2.0
   * - 2.8.x
     - 1.14.0
   * - 2.7.x
     - 1.10.0


Troubleshooting
=====================

See :ref:`troubleshooting:openlineage`.

Where can I learn more?
=======================

- Check out `OpenLineage website <https://openlineage.io>`_.
- Visit our `GitHub repository <https://github.com/OpenLineage/OpenLineage>`_.
- Watch multiple `talks <https://openlineage.io/resources#conference-talks>`_ about OpenLineage.

Feedback
========

You can reach out to us on `slack <http://bit.ly/OpenLineageSlack>`_ and leave us feedback!


How to contribute
=================

We welcome your contributions! OpenLineage is an Open Source project under active development, and we'd love your help!

Sounds fun? Check out our `new contributor guide <https://github.com/OpenLineage/OpenLineage/blob/main/CONTRIBUTING.md>`_ to get started.
