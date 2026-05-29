
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


Informatica Provider API Reference
==================================

This section describes the main public classes and methods provided by the Informatica provider for Apache Airflow.


Hooks
-----

**InformaticaEDCHook**
~~~~~~~~~~~~~~~~~~~~~~

The ``InformaticaEDCHook`` provides low-level access to the Informatica Enterprise Data Catalog (EDC) REST API. It handles authentication, connection configuration, and common EDC operations such as retrieving catalog objects and creating lineage links.

**Initialization Example:**

.. code-block:: python

   from airflow.providers.informatica.hooks.edc import InformaticaEDCHook

   hook = InformaticaEDCHook(informatica_edc_conn_id="my_informatica_conn")

**Key Methods:**

- ``get_object(object_id: str, include_ref_objects: bool = False) -> dict``

  Retrieves a catalog object by its identifier from EDC.

  :param object_id: EDC object identifier (e.g., "``edc://object/table_123``")
  :param include_ref_objects: Whether to include referenced objects (default: False)
  :returns: Dictionary containing object data

  .. code-block:: python

     object_data = hook.get_object("edc://object/table_123")

- ``create_lineage_link(source_object_id: str, target_object_id: str) -> dict``

  Creates a lineage relationship between source and target objects in EDC.

  :param source_object_id: Source object identifier
  :param target_object_id: Target object identifier
  :returns: Dictionary containing operation result

  .. code-block:: python

     result = hook.create_lineage_link("source_id", "target_id")

**InformaticaIDMCHook**
~~~~~~~~~~~~~~~~~~~~~~~

The ``InformaticaIDMCHook`` provides low-level access to IDMC REST APIs. It handles v2 and v3 authentication, session headers for platform and taskflow endpoints, run status normalization, and cancellation requests.

**Initialization Example:**

.. code-block:: python

   from airflow.providers.informatica.hooks.idmc import InformaticaIDMCHook

   hook = InformaticaIDMCHook(informatica_idmc_conn_id="my_idmc_conn", auth_version="v3")

**Key Methods:**

- ``start_task(task_id: str | None = None, task_federated_id: str | None = None, task_type: str = "MTT", callback_url: str | None = None) -> dict``

  Starts a CDI task and returns a dictionary containing the normalized ``run_id``, task metadata, and raw IDMC response.

- ``start_taskflow(taskflow_api_name: str, input_parameters: Mapping[str, Any] | None = None, callback_url: str | None = None) -> dict``

  Starts a published taskflow by API name and returns the taskflow ``run_id``.

- ``get_task_run_status(run_id: str | int, *, task_id: str) -> dict``

  Reads the v2 activity log for a CDI task run and returns a normalized status.

- ``get_taskflow_run_status(run_id: str | int) -> dict``

  Reads taskflow status and returns a normalized status.

- ``cancel_task(...) -> dict`` and ``cancel_taskflow(run_id: str | int) -> dict``

  Send best-effort cancellation requests. ``cancel_task`` sends a task-scoped
  stop request by task identity. ``cancel_taskflow`` terminates a taskflow run
  by run ID.


Operators
---------

**InformaticaIDMCRunTaskOperator**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Starts an IDMC CDI task, optionally waits for it to complete, and returns the IDMC run ID. Supports deferrable execution through ``InformaticaIDMCTaskRunTrigger``.

``cancel_on_kill`` defaults to ``False`` for CDI task runs. Set it to ``True`` only when you want Airflow task termination to send a best-effort IDMC stop request. IDMC stops CDI tasks by task identity rather than by run ID, so this can stop another active run of the same IDMC task if multiple runs overlap.

**InformaticaIDMCRunTaskflowOperator**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Starts an IDMC taskflow by API name, optionally waits for it to complete, and returns the IDMC run ID. Supports deferrable execution through ``InformaticaIDMCTaskflowRunTrigger``.


Sensors
-------

**InformaticaIDMCTaskRunSensor**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Waits for an existing CDI task run to complete. Requires both ``run_id`` and the IDMC ``idmc_task_id`` because the activity log endpoint is filtered by task ID and run ID.

**InformaticaIDMCTaskflowRunSensor**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Waits for an existing IDMC taskflow run to complete.


Extractors
----------

**InformaticaLineageExtractor**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``InformaticaLineageExtractor`` uses an ``InformaticaEDCHook`` to extract lineage information from Informatica EDC and convert it to Airflow-compatible asset definitions. It is typically used internally by the provider's plugin and listeners.

**Initialization Example:**

.. code-block:: python

   from airflow.providers.informatica.extractors.informatica import InformaticaLineageExtractor
   from airflow.providers.informatica.hooks.edc import InformaticaEDCHook

   hook = InformaticaEDCHook()
   extractor = InformaticaLineageExtractor(edc_hook=hook)

**Key Methods:**

- ``get_object(object_id: str) -> dict``

  Returns Informatica catalog object by ID via the EDC hook.

- ``create_lineage_link(source_object_id: str, target_object_id: str) -> dict``

  Creates a lineage link between source and target objects via the EDC hook.


Plugins
-------

**InformaticaProviderPlugin**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``InformaticaProviderPlugin`` registers event listeners that monitor Airflow task lifecycle events (start, success, failure) and trigger lineage extraction and EDC API calls. This plugin is loaded automatically when the provider is installed and enabled.

No manual instantiation is required. The plugin works transparently with any task that defines inlets and outlets.


Configuration Classes
---------------------

**InformaticaConnectionConfig**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This dataclass holds Informatica EDC connection settings, including base URL, credentials, security domain, SSL verification, and provider metadata. It is constructed internally by the hook and not typically used directly by end users.


Error Handling
--------------

**InformaticaEDCError**
~~~~~~~~~~~~~~~~~~~~~~~

Custom exception raised when the Informatica EDC API returns an error or a request fails.

**InformaticaIDMCError**
~~~~~~~~~~~~~~~~~~~~~~~~

Custom exception raised when IDMC authentication, task submission, polling, or cancellation fails.

**IDMCTimeoutException**
~~~~~~~~~~~~~~~~~~~~~~~~

Custom exception raised when an IDMC run does not finish before the configured timeout.


API Endpoints Used
------------------

The Informatica provider uses the following EDC REST API endpoints:

- ``GET /access/2/catalog/data/objects/{object_id}?includeRefObjects={true|false}`` - Retrieve catalog object details
- ``PATCH /access/1/catalog/data/objects`` - Create or update lineage relationships

The IDMC implementation uses platform v2 and taskflow service endpoints including:

- ``POST /api/v2/job`` - Start a CDI task
- ``POST /api/v2/job/stop`` - Stop a CDI task by task identity
- ``GET /api/v2/activity/activityLog`` - Read completed task run status
- ``POST /active-bpel/rt/{taskflow_api_name}`` - Start a taskflow
- ``GET /active-bpel/services/tf/status/{run_id}`` - Read taskflow run status
- ``PUT /active-bpel/services/tf/terminate`` - Terminate a taskflow run

See the configuration and usage guides for more details and complete examples.
