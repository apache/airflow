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

.. _troubleshooting:openlineage:

Troubleshooting
================

.. attention::

    OpenLineage is under active development. Before troubleshooting, please **upgrade to the latest provider and client
    version** and verify that the issue still occurs, as it may have already been resolved in a newer release.


Known Limitations
------------------

Lineage extraction timing out
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For more complex operators (e.g., DbtCloudRunJobOperator), lineage extraction may take longer than the default
limit of 10 seconds. This can cause the OpenLineage extraction process to be killed, either by the listener itself
or by Airflow. You may see log messages like ``OpenLineage process with pid ... expired and will be terminated by listener``.

**Possible Solution**

Increase the :ref:`execution_timeout <config:openlineage__execution_timeout>` configuration in the OpenLineage provider,
as well as the `task_success_overtime <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#task-success-overtime>`_
configuration in Airflow config.


Missing lineage from EmptyOperators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Empty operators are commonly used in Airflow DAGs to "narrow" down the flow, for example as a start/finish mock task
after some dynamic tasks. These operators often lack lineage events because Airflow "optimizes" them by setting their
state directly to SUCCESS without executing the task itself.

**Possible Solution**

To make EmptyOperator scheduled by Airflow and therefore emit OpenLineage events, add callbacks
(``on_execute`` or ``on_success``) or a :ref:`task outlet <inlets_outlets:openlineage>` to it.

If task-level details are not required, you can obtain the state of any task from the DagRun COMPLETE event, which
includes the AirflowStateRunFacet containing state information for all tasks within that DagRun. Note that custom
facets are not emitted outside task execution and thus are not applicable in this scenario.

To determine which operators will emit OpenLineage events ahead of time, DagRun START events contain AirflowJobFacet
with a list of tasks, where each task contains an ``emits_ol_events`` boolean. This checks if the operator is empty,
has callbacks or outlets, and whether task lineage has not been :ref:`selectively disabled <config:openlineage__selective_enable>`
or :ref:`disabled for operator <config:openlineage__disabled_for_operators>`.


Limited lineage from PythonOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

PythonOperator functions as a 'black box' capable of running any code, which might limit the extent of lineage extraction.
While OpenLineage events will still be emitted, additional metadata (like input/output datasets) may be missing.

**Possible Solution**

Use :ref:`supported Hooks <supported_classes:openlineage>` within your PythonOperator to take advantage of Hook Level
Lineage. This feature is available on Airflow 2.10+ and is described in the
`Lineage documentation <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/lineage.html>`_.
By using hooks within Python operators, you can leverage their built-in OpenLineage support to automatically extract
lineage information.


Limited lineage from custom operators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Custom operators may have limited or no OpenLineage support, which can result in gaps in lineage tracking.

**Possible Solution**

Implement OpenLineage methods directly in your custom operator. See the :ref:`guides/developer:openlineage` for
comprehensive documentation on how to add OpenLineage support to custom operators.


Limited lineage from KubernetesPodOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The internals of the KubernetesPodOperator (KPO) are effectively a "black box," complicating lineage tracking with
OpenLineage. The operator runs arbitrary containers, making it difficult to automatically extract lineage information
from the executed workloads. While OpenLineage events will still be emitted, additional metadata (like input/output
datasets) may be missing.

**Possible Solution**

There is no native, supported, or recommended solution for full OpenLineage support for KPO at the moment. However,
there are some possible approaches you can take to fill in gaps in your lineage graph:

- **Manual Annotation**: See :ref:`inlets_outlets:openlineage` for how to manually annotate tasks with datasets.
- **Sidecar Container for Lineage Capture**: Introduce a sidecar container in the KPO pod to capture lineage data
  directly from the operation modifying the data. This setup mirrors the XCom sidecar functionality, where a
  separate container runs alongside the main KPO container, collecting lineage data as it's generated. This data can
  then be forwarded to the OpenLineage backend with additional custom code.


Limited lineage from other provider operators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Certain operators available in other Apache Airflow provider packages may lack OpenLineage support, potentially
limiting tracking capabilities.

**Possible Solution**

OpenLineage support should be added directly for those operators through an open-source contribution
(PR to the Apache Airflow repository). If that's not possible, you can use the :ref:`custom_extractors:openlineage`
feature. See all currently supported classes at :ref:`supported_classes:openlineage`.


Limited lineage from "Schedule External Job" operators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Operators that schedule external jobs (e.g., Spark, Flink) often lack direct access to dataset outputs, capturing only
whether the job succeeded or failed. This limits OpenLineage's ability to track detailed lineage data, especially for
jobs executed on external systems.

**Possible Solutions**

- **Operators with External OpenLineage Integration**: For operators that schedule jobs in systems already supporting
  OpenLineage (e.g., Spark, Trino, Flink, dbt), enable the OpenLineage integration in the target system. Configure the
  Airflow task to inject information about itself as the triggering job, and configure the external system to send
  lineage events to the same OpenLineage endpoint used by Airflow. This allows the external system to emit its own
  lineage events that will be associated with the Airflow task execution. For more details, see :ref:`spark:openlineage`.
- **Operators with Known External Job IDs**: For operators that provide an external job ID, use this ID to retrieve
  lineage data directly. For example, with Databricks operators, call the API to obtain lineage details associated
  with the job ID. This approach can provide lineage data, although it may lack finer details (e.g., column-level lineage).


Troubleshooting steps
---------------------

1. Update the provider and its core dependencies
2. Enable debugging
3. Inspect events locally
4. Perform a quick check of your setup
5. Check for reported bugs in OpenLineage provider and OpenLineage client
6. Before asking for help anywhere, gather all the information listed below that will help diagnose the issue
7. If you are still facing the issue, please open an issue in the Airflow repository.
8. If all else fails, you can try asking for help on the OpenLineage slack channel (but remember that it is a community channel and not a support channel, and people are volunteering their time to help you).


1. Upgrade the provider and its core dependencies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Before troubleshooting any issues, ensure you are using the latest versions of both the
`OpenLineage provider <https://pypi.org/project/apache-airflow-providers-openlineage/>`_ and the
`OpenLineage client <https://pypi.org/project/openlineage-python/>`_.
Read more about the two and the upgrade process in the :ref:`client_v_provider:openlineage`.
Upgrade the OpenLineage provider and the OpenLineage client:

.. code-block:: bash

  pip install --upgrade apache-airflow-providers-openlineage openlineage-python

Then verify the versions in use are the latest available:

.. code-block:: bash

  pip show apache-airflow-providers-openlineage openlineage-python | cat


2. Enable debug settings
^^^^^^^^^^^^^^^^^^^^^^^^^

Enable debug logs by setting the logging level to DEBUG for both Airflow and the OpenLineage client:

- In Airflow, use the `logging_level configuration <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#logging-level>`_ and set the logging level to DEBUG. You can do this, for example, by exporting an environment variable ``AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG``.
- OpenLineage client should automatically pick up the logging level from Airflow, but you can also set it explicitly by exporting an environment variable ``OPENLINEAGE_LOG_LEVEL=DEBUG``.

Enable :ref:`Debug Mode <config:openlineage__debug_mode>` so that the DebugFacet (additional diagnostic info) is attached to events. It can drastically increase the size of the events and logs, so this should only be used temporarily.


3. Inspect events locally
^^^^^^^^^^^^^^^^^^^^^^^^^

With debug logs enabled, raw OpenLineage events will be logged before emitting. Check logs for Openlineage events.

You can also use some simple transport like the ``ConsoleTransport`` to print events to task logs or ``FileTransport`` to save events to json files, e.g.

  .. code-block:: ini

      [openlineage]
      transport = {"type": "console"}

- Or run `Marquez <https://marquezproject.ai/docs/quickstart>`_ locally to inspect whether events are emitted and received.



4. Perform a quick check of your setup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Verify the documentation of provider and `client <https://openlineage.io/docs/client/python>`_, maybe something has changed.
- Configuration present: Ensure a working transport is configured. See :ref:`Transport <config:openlineage__transport>`.
- Disabled settings: Verify you did not disable the integration globally via :ref:`Disabled <config:openlineage__disabled>` or selectively via :ref:`Disabled for operators <config:openlineage__disabled_for_operators>` or :ref:`Selective Enable <config:openlineage__selective_enable>` policy.
- Extraction precedence: If inputs/outputs are missing, remember the order described in :ref:`extraction_precedence:openlineage`.
- Custom extractors registration: If using custom extractors, confirm they are registered via :ref:`Extractors <config:openlineage__extractors>` and importable by both Scheduler and Workers.
- Environment variables: For legacy environments, note the backwards-compatibility env vars in :ref:`Backwards Compatibility <configuration_backwards_compatibility:openlineage>` (e.g., ``OPENLINEAGE_URL``) but prefer Airflow config.


5. Check for common symptoms and fixes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

No events emitted at all:

  - Ensure the provider is installed and at a supported Airflow version (see provider "Requirements").
  - Check :ref:`Disabled <config:openlineage__disabled>` is not set to ``true``.
  - If using selective enablement, verify :ref:`Selective Enable <config:openlineage__selective_enable>` and that the DAG/task is enabled via ``enable_lineage``.
  - Confirm the OpenLineage plugin/listener is loaded in Scheduler/Worker logs.

Events emitted but not received by backend

  - Validate :ref:`Transport <config:openlineage__transport>` or :ref:`Config Path <config:openlineage__config_path>`. See "Transport setup" in :ref:`the configuration section <configuration:openlineage>` and "Configuration precedence".
  - Test with ``ConsoleTransport`` to rule out backend/network issues.
  - Verify network connectivity, auth configuration, and endpoint values.

Inputs/Outputs missing

  - Review :ref:`extraction_precedence:openlineage` and ensure either custom Extractor or Operator OpenLineage methods are implemented.
  - For methods, follow best practices: import OpenLineage-related objects inside the OpenLineage methods, not at module top level; avoid heavy work in ``execute`` that you need in ``_on_start``.
  - For SQL-like operators, ensure relevant job IDs or runtime metadata are available to enrich lineage in ``_on_complete``.

Custom Extractor not working

  - Confirm it's listed under :ref:`Extractors <config:openlineage__extractors>` (or env var equivalent) and importable from both Scheduler and Workers.
  - Avoid cyclical imports: import from Airflow only within ``_execute_extraction``/``extract_on_complete``/``extract_on_failure``, and guard type-only imports with ``typing.TYPE_CHECKING``.
  - Unit test the Extractor to validate ``OperatorLineage`` contents; mock external calls. See example tests referenced in :ref:`custom_extractors:openlineage`.

Custom Run Facets not present

  - Register functions via :ref:`Custom Run Facets <config:openlineage__custom_run_facets>`.
  - Function signature must accept ``TaskInstance`` and ``TaskInstanceState`` and return ``dict[str, RunFacet]`` or ``None``.
  - Avoid duplicate facet keys across functions; duplicates lead to non-deterministic selection.
  - Functions execute on START and COMPLETE/FAIL.

Spark jobs missing parent linkage or transport settings

  - If any ``spark.openlineage.parent*`` or ``spark.openlineage.transport*`` properties are explicitly set in the Spark job config, the integration will not override them.
  - If supported by your Operator, enable :ref:`options:spark_inject_parent_job_info` and :ref:`options:spark_inject_transport_info`

Very large event payloads or serialization failures

  - If :ref:`Include Full Task Info <config:openlineage__include_full_task_info>` is enabled, events may become large; consider disabling or trimming task parameters.
  - :ref:`Disable Source Code <config:openlineage__disable_source_code>` can reduce payloads for Python/Bash operators that include source code by default.


6. Check for open bugs and issues in the provider and the client
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Check for open bugs and issues in the provider and the client.

- Provider: https://github.com/apache/airflow-provider-openlineage/issues
- Client: https://github.com/OpenLineage/OpenLineage/issues


7. Gather crucial information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- Airflow scheduler logs (with log level set to DEBUG, see Step 2 above)
- Airflow worker (task) logs (with log level set to DEBUG, see Step 2 above)
- OpenLineage events (with :ref:`Debug Mode <config:openlineage__debug_mode>` enabled)
- Airflow version, OpenLineage provider version and OpenLineage client version
- Details on any custom deployment/environment modifications


8. Open an issue on the Airflow repository
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are still facing the issue, please open an issue in the `Airflow repository <https://github.com/apache/airflow/issues>`_ and include all the information **gathered in the previous step** together with a simple example on how to reproduce the issue. Do not paste your entire codebase, try to come up with a simple code that will demonstrate the problem - this increases the chances of the bug getting fixed quickly.


9. Ask for help on the OpenLineage slack channel
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If all else fails, you can try asking for help on the OpenLineage `slack channel <http://bit.ly/OpenLineageSlack>`_ (but remember that it is a community channel and not a support channel, and people are volunteering their time to help you).
