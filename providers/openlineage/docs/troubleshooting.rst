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

1. Update the provider and its core dependencies
2. Enable debugging
3. Inspect events locally
4. Perform a quick check of your setup
5. Check for reported bugs in OpenLineage provider and OpenLineage client
6. Before asking for help anywhere, gather all the information listed below that will help diagnose the issue
7. If you are still facing the issue, please open an issue on the provider repository.
8. If all else fails, you can try asking for help on the OpenLineage slack channel (but remember that it is a community channel and not a support channel, and people are volunteering their time to help you).


1. Upgrade the provider and its core dependencies
--------------------------------------------------

Upgrade the OpenLineage provider and the OpenLineage client. If you'd like to know the difference between the two, you can read more about it in the :ref:`client_v_provider:openlineage`.

.. code-block:: bash

  pip install --upgrade apache-airflow-providers-openlineage openlineage-python

Then verify the versions in use are the latest available:

.. code-block:: bash

  pip show apache-airflow-providers-openlineage openlineage-python | cat


2. Enable debug settings
-------------------------

Enable debug logs by setting the logging level to DEBUG for both Airflow and the OpenLineage client:

- In Airflow, use the `logging_level configuration <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#logging-level>`_ and set the logging level to DEBUG. You can do this f.e. by exporting and env variable ``AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG``.
- OpenLineage client should automatically pick up the logging level from Airflow, but you can also set it explicitly by exporting and env variable ``OPENLINEAGE_LOG_LEVEL=DEBUG``.

Enable :ref:`Debug Mode <config:openlineage__debug_mode>` so that the DebugFacet (additional diagnostic info) is attached to events. It can drastically increase the size of the events and logs, so this should only be used temporarily.


3. Inspect events locally
--------------------------

With debug logs enabled, raw OpenLineage events will be logged before emitting. Check logs for Openlineage events.

You can also use some simple transport like the ``ConsoleTransport`` to print events to task logs or ``FileTransport`` to save events to json files, e.g.

  .. code-block:: ini

      [openlineage]
      transport = {"type": "console"}

- Or run `Marquez <https://marquezproject.ai/docs/quickstart>`_ locally to inspect whether events are emitted and received.



4. Perform a quick check of your setup
--------------------------------------

- Verify the documentation of provider and `client <https://openlineage.io/docs/client/python>`_, maybe something has changed.
- Configuration present: Ensure a working transport is configured. See :ref:`Transport <config:openlineage__transport>`.
- Disabled settings: Verify you did not disable the integration globally via :ref:`Disabled <config:openlineage__disabled>` or selectively via :ref:`Disabled for operators <config:openlineage__disabled_for_operators>` or :ref:`Selective Enable <config:openlineage__selective_enable>` policy.
- Extraction precedence: If inputs/outputs are missing, remember the order described in :ref:`extraction_precedence:openlineage`.
- Custom extractors registration: If using custom extractors, confirm they are registered via :ref:`Extractors <config:openlineage__extractors>` and importable by both Scheduler and Workers.
- Environment variables: For legacy environments, note the backwards-compatibility env vars in :ref:`Backwards Compatibility <configuration_backwards_compatibility:openlineage>` (e.g., ``OPENLINEAGE_URL``) but prefer Airflow config.


5. Check for common symptoms and fixes
--------------------------------------

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
----------------------------------------------------------------

Check for open bugs and issues in the provider and the client.

- Provider: https://github.com/apache/airflow-provider-openlineage/issues
- Client: https://github.com/OpenLineage/OpenLineage/issues


7. Gather crucial information
-----------------------------
- Airflow scheduler logs (with log level set to DEBUG, see Step 2 above)
- Airflow worker (task) logs (with log level set to DEBUG, see Step 2 above)
- OpenLineage events (with :ref:`Debug Mode <config:openlineage__debug_mode>` enabled)
- Airflow version, OpenLineage provider version and OpenLineage client version
- Details on any custom deployment/environment modifications


8. Open an issue on the provider repository
-------------------------------------------

If you are still facing the issue, please open an issue on the provider repository and include all the information **gathered in the previous step** together with a simple example on how to reproduce the issue. Do not paste your entire codebase, try to come up with a simple code that will demonstrate the problem - this increase chances of bug getting fixed quickly.


9. Ask for help on the OpenLineage slack channel
------------------------------------------------

If all else fails, you can try asking for help on the OpenLineage `slack channel <http://bit.ly/OpenLineageSlack>`_ (but remember that it is a community channel and not a support channel, and people are volunteering their time to help you).
