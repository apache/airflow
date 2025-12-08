
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

.. _guides/user:openlineage:

Using OpenLineage integration
-----------------------------

OpenLineage is an open framework for data lineage collection and analysis. At its core is an extensible specification that systems can use to interoperate with lineage metadata.
`Check out OpenLineage docs <https://openlineage.io/docs/>`_.

**No change to user Dag files is required to use OpenLineage**. Basic configuration is needed so that OpenLineage knows where to send events.

Quickstart
==========

.. note::

    OpenLineage Provider offers a diverse range of data transport options (http, kafka, file etc.),
    including the flexibility to create a custom solution. Configuration can be managed through several approaches
    and there is an extensive array of settings available for users to fine-tune and enhance their use of OpenLineage.
    For a comprehensive explanation of these features, please refer to the subsequent sections of this document.

This example is a basic demonstration of OpenLineage setup.

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

3. **That's it !**  OpenLineage events should be sent to the configured backend when Dags are run.

Usage
=====

When enabled and configured, the integration requires no further action from the user. It will automatically:

- Collect task input / output metadata (source, schema, etc.).
- Collect task run-level metadata (execution time, state, parameters, etc.)
- Collect task job-level metadata (owners, type, description, etc.)
- Collect task-specific metadata (bigquery job id, python source code, etc.) - depending on the Operator

All this data will be sent as OpenLineage events to the configured backend as described in :ref:`job_hierarchy:openlineage`.

Transport setup
===============

Primary, and recommended method of configuring OpenLineage Airflow Provider is Airflow configuration (``airflow.cfg`` file).
All possible configuration options, with example values, can be found in :ref:`the configuration section <configuration:openlineage>`.

At minimum, one thing that needs to be set up in every case is ``Transport`` - where do you wish for
your events to end up - for example `Marquez <https://marquezproject.ai/>`_.

Transport as JSON string
^^^^^^^^^^^^^^^^^^^^^^^^
The ``transport`` option in Airflow configuration is used for that purpose.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}

``AIRFLOW__OPENLINEAGE__TRANSPORT`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__TRANSPORT='{"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}'


If you want to look at OpenLineage events without sending them anywhere, you can set up ``ConsoleTransport`` - the events will end up in task logs.

.. code-block:: ini

    [openlineage]
    transport = {"type": "console"}

.. note::
  For full list of built-in transport types, specific transport's options or instructions on how to implement your custom transport, refer to
  `Python client documentation <https://openlineage.io/docs/client/python#built-in-transport-types>`_.

Transport as config file
^^^^^^^^^^^^^^^^^^^^^^^^
You can also configure OpenLineage ``Transport`` using a YAML file (f.e. ``openlineage.yml``).
Provide the path to the YAML file as ``config_path`` option in Airflow configuration.

.. code-block:: ini

    [openlineage]
    config_path = '/path/to/openlineage.yml'

``AIRFLOW__OPENLINEAGE__CONFIG_PATH`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__CONFIG_PATH='/path/to/openlineage.yml'

Example content of config YAML file:

.. code-block:: ini

  transport:
    type: http
    url: https://backend:5000
    endpoint: events/receive
    auth:
      type: api_key
      apiKey: f048521b-dfe8-47cd-9c65-0cb07d57591e

.. note::

    Detailed description of that configuration method, together with example config files,
    can be found `in Python client documentation <https://openlineage.io/docs/client/python#built-in-transport-types>`_.

Configuration precedence
^^^^^^^^^^^^^^^^^^^^^^^^

As there are multiple possible ways of configuring OpenLineage, it's important to keep in mind the precedence of different configurations.
OpenLineage Airflow Provider looks for the configuration in the following order:

1. Check ``config_path`` in ``airflow.cfg`` under ``openlineage`` section (or AIRFLOW__OPENLINEAGE__CONFIG_PATH environment variable)
2. Check ``transport`` in ``airflow.cfg`` under ``openlineage`` section (or AIRFLOW__OPENLINEAGE__TRANSPORT environment variable)
3. If all the above options are missing, the OpenLineage Python client used underneath looks for configuration in the order described in `this <https://openlineage.io/docs/client/python#configuration>`_ documentation. Please note that **using Airflow configuration is encouraged** and is the only future proof solution.

Backwards compatibility
^^^^^^^^^^^^^^^^^^^^^^^

.. warning::

  Below variables **should not** be used and can be removed in the future. Consider using Airflow configuration (described above) for a future proof solution.

For backwards compatibility with ``openlineage-airflow`` package, some environment variables are still available:

- ``OPENLINEAGE_DISABLED`` is an equivalent of ``AIRFLOW__OPENLINEAGE__DISABLED``.
- ``OPENLINEAGE_CONFIG`` is an equivalent of ``AIRFLOW__OPENLINEAGE__CONFIG_PATH``.
- ``OPENLINEAGE_NAMESPACE`` is an equivalent of ``AIRFLOW__OPENLINEAGE__NAMESPACE``.
- ``OPENLINEAGE_EXTRACTORS`` is an equivalent of setting ``AIRFLOW__OPENLINEAGE__EXTRACTORS``.
- ``OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE`` is an equivalent of ``AIRFLOW__OPENLINEAGE__DISABLE_SOURCE_CODE``.
- ``OPENLINEAGE_URL`` can be used to set up simple http transport. This method has some limitations and may require using other environment variables to achieve desired output. See `docs <https://openlineage.io/docs/client/python#http-transport-configuration-with-environment-variables>`_.


Additional Options
==================

Namespace
^^^^^^^^^

It's very useful to set up OpenLineage namespace for this particular instance.
That way, if you use multiple OpenLineage producers, events coming from them will be logically separated.
If not set, it's using ``default`` namespace. Provide the name of the namespace as ``namespace`` option in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    namespace = 'my-team-airflow-instance'

``AIRFLOW__OPENLINEAGE__NAMESPACE`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__NAMESPACE='my-team-airflow-instance'

Timeout
^^^^^^^

To add a layer of isolation between task execution and OpenLineage, adding a level of assurance that OpenLineage execution does not
interfere with task execution in a way other than taking time, OpenLineage methods run in separate process.
The code runs with default timeout of 10 seconds. You can increase this by setting the ``execution_timeout`` value.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    execution_timeout = 60

``AIRFLOW__OPENLINEAGE__EXECUTION_TIMEOUT`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__EXECUTION_TIMEOUT=60

.. _options:disable:

Disable
^^^^^^^
You can disable sending OpenLineage events without uninstalling OpenLineage provider by setting
``disabled`` option to ``true`` in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    disabled = true

``AIRFLOW__OPENLINEAGE__DISABLED`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__DISABLED=true


Disable source code
^^^^^^^^^^^^^^^^^^^

Several Operators (f.e. Python, Bash) will by default include their source code in their OpenLineage events.
To prevent that, set ``disable_source_code`` option to ``true`` in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    disable_source_code = true

``AIRFLOW__OPENLINEAGE__DISABLE_SOURCE_CODE`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__DISABLE_SOURCE_CODE=true


Disabled for Operators
^^^^^^^^^^^^^^^^^^^^^^

You can easily exclude some Operators from emitting OpenLineage events by passing a string of semicolon separated
full import paths of Airflow Operators to disable as ``disabled_for_operators`` field in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    disabled_for_operators = 'airflow.providers.standard.operators.bash.BashOperator;airflow.providers.standard.operators.python.PythonOperator'

``AIRFLOW__OPENLINEAGE__DISABLED_FOR_OPERATORS`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__DISABLED_FOR_OPERATORS='airflow.providers.standard.operators.bash.BashOperator;airflow.providers.standard.operators.python.PythonOperator'

Full Task Info
^^^^^^^^^^^^^^

By default, OpenLineage integration's AirflowRunFacet - attached on START event for every task instance event - does
not contain full serialized task information (parameters to given operator), but only includes select parameters.

However, we allow users to set OpenLineage integration to include full task information. By doing this, rather than
serializing only a few known attributes, we exclude certain non-serializable elements and send everything else.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    include_full_task_info = true

``AIRFLOW__OPENLINEAGE__INCLUDE_FULL_TASK_INFO`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__INCLUDE_FULL_TASK_INFO=true

.. warning::

  By setting this variable to true, OpenLineage integration does not control the size of event you sent. It can potentially include elements that are megabytes in size or larger, depending on the size of data you pass to the task.


Custom Extractors
^^^^^^^^^^^^^^^^^

To use :ref:`custom Extractors <custom_extractors:openlineage>` feature, register the extractors by passing
a string of semicolon separated Airflow Operators full import paths to ``extractors`` option in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    extractors = full.path.to.ExtractorClass;full.path.to.AnotherExtractorClass

``AIRFLOW__OPENLINEAGE__EXTRACTORS`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__EXTRACTORS='full.path.to.ExtractorClass;full.path.to.AnotherExtractorClass'

Custom Run Facets
^^^^^^^^^^^^^^^^^

To inject :ref:`custom run facets <custom_facets:openlineage>`, register the custom run facet functions by passing
a string of semicolon separated full import paths to ``custom_run_facets`` option in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    custom_run_facets = full.path.to.get_my_custom_facet;full.path.to.another_custom_facet_function

``AIRFLOW__OPENLINEAGE__CUSTOM_RUN_FACETS`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__CUSTOM_RUN_FACETS='full.path.to.get_my_custom_facet;full.path.to.another_custom_facet_function'

.. _options:debug_mode:

Debug Mode
^^^^^^^^^^

You can enable sending additional information in OpenLineage events that can be useful for debugging and
reproducing your environment setup by setting ``debug_mode`` option to ``true`` in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    debug_mode = true

``AIRFLOW__OPENLINEAGE__DEBUG_MODE`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__DEBUG_MODE=true

.. warning::

  By setting this variable to true, OpenLineage integration may log and emit extensive details. It should only be enabled temporary for debugging purposes.


Enabling OpenLineage on Dag/task level
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

One can selectively enable OpenLineage for specific Dags and tasks by using the ``selective_enable`` policy.
To enable this policy, set the ``selective_enable`` option to True in the [openlineage] section of your Airflow configuration file:

.. code-block:: ini

    [openlineage]
    selective_enable = True

``AIRFLOW__OPENLINEAGE__SELECTIVE_ENABLE`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__SELECTIVE_ENABLE=true


While ``selective_enable`` enables selective control, the ``disabled`` :ref:`option <options:disable>` still has precedence.
If you set ``disabled`` to True in the configuration, OpenLineage will be disabled for all Dags and tasks regardless of the ``selective_enable`` setting.

Once the ``selective_enable`` policy is enabled, you can choose to enable OpenLineage
for individual Dags and tasks using the ``enable_lineage`` and ``disable_lineage`` functions.

1. Enabling Lineage on a Dag:

.. code-block:: python

    from airflow.providers.openlineage.utils.selective_enable import disable_lineage, enable_lineage

    with enable_lineage(Dag(...)):
        # Tasks within this Dag will have lineage tracking enabled
        MyOperator(...)

        AnotherOperator(...)

2. Enabling Lineage on a Task:

While enabling lineage on a Dag implicitly enables it for all tasks within that Dag, you can still selectively disable it for specific tasks:

.. code-block:: python

    from airflow.providers.openlineage.utils.selective_enable import disable_lineage, enable_lineage

    with DAG(...) as dag:
        t1 = MyOperator(...)
        t2 = AnotherOperator(...)

    # Enable lineage for the entire Dag
    enable_lineage(dag)

    # Disable lineage for task t1
    disable_lineage(t1)

Enabling lineage on the Dag level automatically enables it for all tasks within that Dag unless explicitly disabled per task.

Enabling lineage on the task level implicitly enables lineage on its Dag.
This is because each emitting task sends a `ParentRunFacet <https://openlineage.io/docs/spec/facets/run-facets/parent_run>`_,
which requires the Dag-level lineage to be enabled in some OpenLineage backend systems.
Disabling Dag-level lineage while enabling task-level lineage might cause errors or inconsistencies.

.. _options:spark_inject_parent_job_info:

Passing parent job information to Spark jobs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

OpenLineage integration can automatically inject Airflow's information (namespace, job name, run id)
into Spark application properties as parent job information
(``spark.openlineage.parentJobNamespace``, ``spark.openlineage.parentJobName``, ``spark.openlineage.parentRunId``),
for :ref:`supported Operators <supported_classes:openlineage>`.
It allows Spark integration to automatically include ``parentRunFacet`` in application-level OpenLineage event,
creating a parent-child relationship between tasks from different integrations.
See `Scheduling from Airflow <https://openlineage.io/docs/integrations/spark/configuration/airflow>`_.

This configuration serves as the default behavior for all Operators that support automatic Spark properties injection,
unless it is explicitly overridden at the Operator level.
To prevent a specific Operator from injecting the parent job information while
allowing all other supported Operators to do so by default, ``openlineage_inject_parent_job_info=False``
can be explicitly provided to that specific Operator.

.. note::

  If any of the ``spark.openlineage.parent*`` properties are manually specified in the Spark job configuration, the integration will refrain from injecting parent job properties to ensure that manually provided values are preserved.

You can enable this automation by setting ``spark_inject_parent_job_info`` option to ``true`` in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    spark_inject_parent_job_info = true

``AIRFLOW__OPENLINEAGE__SPARK_INJECT_PARENT_JOB_INFO`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__SPARK_INJECT_PARENT_JOB_INFO=true


Passing transport information to Spark jobs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

OpenLineage integration can automatically inject Airflow's transport information into Spark application properties,
for :ref:`supported Operators <supported_classes:openlineage>`.
It allows Spark integration to send events to the same backend as Airflow integration without manual configuration.
See `Scheduling from Airflow <https://openlineage.io/docs/integrations/spark/configuration/airflow>`_.

.. note::

  If any of the ``spark.openlineage.transport*`` properties are manually specified in the Spark job configuration, the integration will refrain from injecting transport properties to ensure that manually provided values are preserved.

You can enable this automation by setting ``spark_inject_transport_info`` option to ``true`` in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    spark_inject_transport_info = true

``AIRFLOW__OPENLINEAGE__SPARK_INJECT_TRANSPORT_INFO`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__SPARK_INJECT_TRANSPORT_INFO=true


Passing parent information to Airflow DAG
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable full OpenLineage lineage tracking across dependent DAGs, you can pass parent and root job information
through the DAG's ``dag_run.conf``. When a DAG run configuration includes an ``openlineage`` section with valid metadata,
this information is automatically parsed and converted into DAG run's ``parentRunFacet``, from which the root information
is also propagated to all tasks. If no DAG run ``openlineage`` configuration is provided, the DAG run will not contain
``parentRunFacet`` and root of all tasks will default to Dag run.

The ``openlineage`` dict in conf should contain the following keys:


*(all three values must be included to create a parent reference)*

- **parentRunId** — the unique run ID (uuid) of the direct parent job
- **parentJobName** — the name of the parent job
- **parentJobNamespace** — the namespace of the parent job

*(all three values must be included to create a root reference, otherwise parent will be used as root)*

- **rootParentRunId** — the run ID (uuid) of the top-level (root) job
- **rootParentJobName** — the name of the top-level (root) job
- **rootParentJobNamespace** — the namespace of the top-level (root) job

.. note::

  We highly recommend providing all six OpenLineage identifiers (parent and root) to ensure complete lineage tracking. If the root information is missing, the parent set will be used as the root; if any of the three parent fields are missing, no parent facet will be created. Partial or mixed configurations are not supported - either all three parent or all three root values must be provided together.


Example:

.. code-block:: shell

    curl -X POST "http://<AIRFLOW_HOST>/api/v2/dags/my_dag_name/dagRuns" \
    -H "Content-Type: application/json" \
    -d '{
      "logical_date": "2019-08-24T14:15:22Z",
      "conf": {
        "openlineage": {
          "parentRunId": "3bb703d1-09c1-4a42-8da5-35a0b3216072",
          "parentJobNamespace": "prod_biz",
          "parentJobName": "get_files",
          "rootParentRunId": "9d3b14f7-de91-40b6-aeef-e887e2c7673e",
          "rootParentJobNamespace": "prod_analytics",
          "rootParentJobName": "generate_report_sales_e2e"
        }
      }
    }'


Troubleshooting
===============

See :ref:`troubleshooting:openlineage` for details on how to troubleshoot OpenLineage.


Adding support for custom Operators
===================================

If you want to add OpenLineage coverage for particular Operator, take a look at :ref:`guides/developer:openlineage`


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
