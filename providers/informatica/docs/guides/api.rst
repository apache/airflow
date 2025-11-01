
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


Operators
---------

**EmptyOperator**
~~~~~~~~~~~~~~~~~

The ``EmptyOperator`` is a no-op operator that can be used to group tasks or test lineage extraction. It supports ``inlets`` and ``outlets`` for lineage tracking.

**Example Usage:**

.. code-block:: python

   from airflow.providers.informatica.operators.empty import EmptyOperator

   empty_task = EmptyOperator(
       task_id="empty_task",
       inlets=[{"dataset_uri": "edc://object/test_table"}],
       outlets=[{"dataset_uri": "edc://object/result_table"}],
   )

**Key Parameters:**

- ``inlets``: List of input dataset URIs (e.g., Informatica EDC object URIs)
- ``outlets``: List of output dataset URIs


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


EDC API Endpoints Used
----------------------

The Informatica provider uses the following EDC REST API endpoints:

- ``GET /access/2/catalog/data/objects/{object_id}?includeRefObjects={true|false}`` — Retrieve catalog object details
- ``PATCH /access/1/catalog/data/objects`` — Create or update lineage relationships

See the configuration and usage guides for more details and complete examples.
