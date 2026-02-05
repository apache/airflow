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

.. _spark:openlineage:

OpenLineage Spark Integration
==============================

The OpenLineage Spark integration is a **separate package** from the Airflow OpenLineage provider.
For Spark applications to send lineage (regardless if triggered from Airflow or not),
you need to have the Spark OpenLineage integration enabled and configured in your Spark application.
The Airflow OpenLineage provider can, in some cases, help facilitate this by automatically injecting necessary configuration into Spark jobs.
Usually, the same parameters that are passed to spark-submit can also be supplied directly from Airflow and other schedulers,
allowing for seamless configuration and execution of Spark jobs.

Understanding different integrations
-------------------------------------

**Airflow OpenLineage Provider**

Tracks lineage for Airflow DAGs and tasks. It emits OpenLineage events when Airflow tasks execute,
capturing information about the Airflow workflow itself. It must be installed and enabled in your Airflow environment.

**Spark OpenLineage Integration**

Tracks lineage for Spark applications. It uses the ``OpenLineageSparkListener`` to monitor Spark
execution and extract metadata about datasets, jobs, and their dependencies. This integration
must be enabled in your Spark application independently.

**When Spark jobs are triggered from Airflow, both integrations work together**

- The Airflow OpenLineage provider tracks the Airflow task that triggers the Spark job
- The Spark integration tracks the actual Spark application execution
- Parent job information injected into Spark application by Airflow task links the two jobs together

For detailed information about the Spark integration, see the
`OpenLineage Spark documentation <https://openlineage.io/docs/integrations/spark/>`_.

Enabling Spark OpenLineage Integration
---------------------------------------

To enable OpenLineage in your Spark application, you need to install and configure the OpenLineage Spark
integration. **This is a separate step from enabling the Airflow OpenLineage provider.**
For detailed installation instructions, including different installation methods, see the
`OpenLineage Spark Installation documentation <https://openlineage.io/docs/integrations/spark/installation>`_.
After installation, you'll need to configure the Spark OpenLineage listener and other settings.
For complete Spark configuration options, see the
`Spark Configuration documentation <https://openlineage.io/docs/integrations/spark/configuration/>`_.

Preserving Job Hierarchy
-------------------------

To establish a correct job hierarchy in lineage tracking, the Spark application needs to know about
its parent job (e.g., the Airflow task that triggered it). This allows the Spark integration to automatically
add a ``ParentRunFacet`` to the Spark application-level OpenLineage event, linking the Spark job to its
originating Airflow job in the lineage graph.

For a general explanation of why preserving job hierarchy is important and how it works, see :ref:`howto/macros:openlineage`.

The following Spark properties are required for automatic creation of the ``ParentRunFacet``:

- ``spark.openlineage.parentJobNamespace`` - Namespace of the parent job (Airflow task)
- ``spark.openlineage.parentJobName`` - Job name of the parent job (Airflow task)
- ``spark.openlineage.parentRunId`` - Run ID of the parent job (Airflow task)

Additionally, the following properties (available in Spark integration version 1.31.0 and later) allow
easier connection of the root (top-level parent) job to the children jobs:

- ``spark.openlineage.rootParentJobNamespace`` - Namespace of the root job (e.g., Airflow DAG)
- ``spark.openlineage.rootParentJobName`` - Job name of the root job (e.g., Airflow DAG)
- ``spark.openlineage.rootParentRunId`` - Run ID of the root job (e.g., Airflow DAG)


Automatic Injection
-------------------

The Airflow OpenLineage provider can automatically inject parent job information and transport configuration
into Spark application properties when Spark jobs are submitted from Airflow. This eliminates the need
to manually configure these properties in every Spark operator.

Automatic injection is supported for the following operators:

- :class:`~airflow.providers.apache.livy.operators.livy.LivyOperator`
- :class:`~airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator`
- :class:`~airflow.providers.google.cloud.operators.dataproc.DataprocCreateBatchOperator`
- :class:`~airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateInlineWorkflowTemplateOperator`
- :class:`~airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator`


.. _options:spark_inject_parent_job_info:

Enabling Automatic Parent Job Information Injection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Airflow OpenLineage provider can automatically inject Airflow's task information into Spark application properties as parent job information.

This configuration serves as the default behavior for all Operators that support automatic Spark properties injection,
unless it is explicitly overridden at the Operator level. To prevent a specific Operator from injecting the parent job information while
allowing all other supported Operators to do so by default, ``openlineage_inject_parent_job_info=False``
can be explicitly provided to that specific Operator.

You can enable this automation by setting ``spark_inject_parent_job_info`` option to ``true`` in Airflow configuration:

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    spark_inject_parent_job_info = true

``AIRFLOW__OPENLINEAGE__SPARK_INJECT_PARENT_JOB_INFO`` environment variable is an equivalent:

.. code-block:: bash

    export AIRFLOW__OPENLINEAGE__SPARK_INJECT_PARENT_JOB_INFO=true

When enabled, the following properties are automatically injected into Spark job configuration:

- ``spark.openlineage.parentJobNamespace``
- ``spark.openlineage.parentJobName``
- ``spark.openlineage.parentRunId``
- ``spark.openlineage.rootParentJobNamespace``
- ``spark.openlineage.rootParentJobName``
- ``spark.openlineage.rootParentRunId``

.. note::

    If any of the ``spark.openlineage.parent*`` properties are manually specified in the Spark job
    configuration, the integration will refrain from injecting parent job properties to ensure that manually
    provided values are preserved.


.. _options:spark_inject_transport_info:

Enabling Automatic Transport Information Injection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Airflow OpenLineage provider can automatically inject Airflow's transport information into Spark application properties.
When enabled, the transport configuration from Airflow (URL, authentication, etc.) is automatically
injected into Spark job configuration, allowing Spark OpenLineage integration to send events to the same OpenLineage backend
as Airflow without manual configuration.

.. caution::

    Currently, only HTTP transport is supported for automatic transport injection (with api_key authentication, if configured).


.. note::

    Ensure that the OpenLineage backend is accessible from the Spark execution environment.
    Depending on where your Spark jobs run (e.g., on-premises clusters, cloud environments, isolated networks),
    you may need to configure network access, proxies, or firewall rules to allow Spark applications to reach the same backend as Airflow environment.

This configuration serves as the default behavior for all Operators that support automatic Spark properties injection,
unless it is explicitly overridden at the Operator level. To prevent a specific Operator from injecting the transport information while
allowing all other supported Operators to do so by default, ``openlineage_inject_transport_info=False``
can be explicitly provided to that specific Operator.

You can enable this automation by setting ``spark_inject_transport_info`` option to ``true`` in Airflow configuration:

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    spark_inject_transport_info = true

``AIRFLOW__OPENLINEAGE__SPARK_INJECT_TRANSPORT_INFO`` environment variable is an equivalent:

.. code-block:: bash

    export AIRFLOW__OPENLINEAGE__SPARK_INJECT_TRANSPORT_INFO=true

.. note::

    If any of the ``spark.openlineage.transport*`` properties are manually specified in the Spark job configuration,
    the integration will refrain from injecting transport properties to ensure that manually provided values are preserved.


Per-Operator Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^

You can override the global configuration on a per-operator basis using operator parameters.
This allows you to customize the injection behavior for specific operators while maintaining the default behavior for others:

.. code-block:: python

    SparkSubmitOperator(
        task_id="my_task",
        application="/path/to/app.py",
        openlineage_inject_parent_job_info=True,  # Override global setting
        openlineage_inject_transport_info=False,  # Disable for this operator
        conf={
            # Your Spark configuration
        },
    )



Complete Example
-----------------

Here's a complete example using ``DataprocSubmitJobOperator`` with automatic injection enabled:

.. code-block:: python

    from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

    spark_job = DataprocSubmitJobOperator(
        task_id="process_data",
        project_id="my-project",
        region="us-central1",
        job={
            "reference": {"project_id": "my-project"},
            "placement": {"cluster_name": "my-cluster"},
            "pyspark_job": {
                "main_python_file_uri": "gs://bucket/my-spark-app.py",
                "properties": {
                    # Spark OpenLineage listener and jar
                    "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
                    "spark.jars.packages": "io.openlineage:openlineage-spark_${SCALA_BINARY_VERSION}:1.41.0",
                    # Transport configuration will be automatically injected if spark_inject_transport_info is enabled
                    # Parent and root information will be automatically injected if spark_inject_parent_job_info is enabled
                },
            },
        },
        dag=dag,
    )

With automatic injection enabled, the parent job information and transport configuration are added
automatically, so you only need to configure the Spark OpenLineage listener and namespace.


Manual Configuration Using Macros
-----------------------------------

If you need more control over the configuration that Airflow injects into Spark application, you can use the OpenLineage macros.
See :ref:`howto/macros:openlineage`.

Example with manual configuration:

.. code-block:: python

    SparkSubmitOperator(
        task_id="my_task",
        application="/path/to/spark/app.py",
        conf={
            # Spark OpenLineage listener and packages
            "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
            "spark.jars.packages": "io.openlineage:openlineage-spark_${SCALA_BINARY_VERSION}:1.41.0",
            # Spark OpenLineage namespace
            "spark.openlineage.namespace": "my-spark-namespace",
            # Transport configuration
            "spark.openlineage.transport.type": "http",
            "spark.openlineage.transport.url": "http://openlineage-backend:5000",
            "spark.openlineage.transport.endpoint": "api/v1/lineage",
            "spark.openlineage.transport.auth.type": "api_key",
            "spark.openlineage.transport.auth.apiKey": "your-api-key",
            # Parent job information (using macros)
            "spark.openlineage.parentJobNamespace": "{{ macros.OpenLineageProviderPlugin.lineage_job_namespace() }}",
            "spark.openlineage.parentJobName": "{{ macros.OpenLineageProviderPlugin.lineage_job_name(task_instance) }}",
            "spark.openlineage.parentRunId": "{{ macros.OpenLineageProviderPlugin.lineage_run_id(task_instance) }}",
            # Root parent job information (using macros)
            "spark.openlineage.rootParentJobNamespace": "{{ macros.OpenLineageProviderPlugin.lineage_root_job_namespace(task_instance) }}",
            "spark.openlineage.rootParentJobName": "{{ macros.OpenLineageProviderPlugin.lineage_root_job_name(task_instance) }}",
            "spark.openlineage.rootParentRunId": "{{ macros.OpenLineageProviderPlugin.lineage_root_run_id(task_instance) }}",
        },
    )
