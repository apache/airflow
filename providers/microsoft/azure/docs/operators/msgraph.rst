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


Microsoft Graph API Operators
=============================

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:MSGraphAsyncOperator:

MSGraphAsyncOperator
----------------------------------
Use the
:class:`~airflow.providers.microsoft.azure.operators.msgraph.MSGraphAsyncOperator` to call Microsoft Graph API.


Below is an example of using this operator to get a Sharepoint site.

.. exampleinclude:: /../tests/system/microsoft/azure/example_msgraph.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_graph_site]
    :end-before: [END howto_operator_graph_site]

Below is an example of using this operator to get a Sharepoint site pages.

.. exampleinclude:: /../tests/system/microsoft/azure/example_msgraph.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_graph_site_pages]
    :end-before: [END howto_operator_graph_site_pages]

Below is an example of using this operator to get PowerBI workspaces.

.. exampleinclude:: /../tests/system/microsoft/azure/example_powerbi.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_powerbi_workspaces]
    :end-before: [END howto_operator_powerbi_workspaces]

Below is an example of using this operator to get PowerBI workspaces info.

.. exampleinclude:: /../tests/system/microsoft/azure/example_powerbi.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_powerbi_workspaces_info]
    :end-before: [END howto_operator_powerbi_workspaces_info]

Below is an example of using this operator to refresh PowerBI dataset.

.. exampleinclude:: /../tests/system/microsoft/azure/example_powerbi.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_powerbi_refresh_dataset]
    :end-before: [END howto_operator_powerbi_refresh_dataset]

Below is an example of using this operator to create an item schedule in Fabric.

.. exampleinclude:: /../tests/system/microsoft/azure/example_msfabric.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_ms_fabric_create_item_schedule]
    :end-before: [END howto_operator_ms_fabric_create_item_schedule]

.. _howto/operator:MSGraphAsyncOperator:start_from_trigger:

Starting from the triggerer
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The operator does all its requests in the triggerer, so the only thing it does on a worker at the start
is to hand the first request over. With ``start_from_trigger=True`` the scheduler defers the task itself
and that first worker run is skipped. The worker gets involved once the response is there, to process it
and to follow the pagination.

.. code-block:: python

    site_task = MSGraphAsyncOperator(
        task_id="news_site",
        conn_id="msgraph_api",
        url="sites/{{ params.site }}",
        start_from_trigger=True,
    )

This needs Airflow 3.3 or later, on older versions the task starts on a worker as it does without the
argument. It also starts on a worker when:

* an argument which is passed on to the trigger is an ``XComArg``, a callable or a file-like object,
  as the triggerer cannot resolve or receive those;
* the task is mapped with ``expand``.

Keep the following in mind when you enable it:

* The templated fields of the first request are rendered by the triggerer, which does not know the
  ``user_defined_macros`` and ``user_defined_filters`` of the Dag and always renders strings, also when
  ``render_template_as_native_obj`` is set.
* The task goes straight from scheduled to deferred, so the first request does not wait for a pool
  slot or for a worker.

Cross-References
----------------

* :doc:`Microsoft Graph Filesystem </filesystems/msgraph>` - For file operations using ObjectStoragePath

Reference
---------

For further information, look at:

* `Use the Microsoft Graph API <https://learn.microsoft.com/en-us/graph/use-the-api/>`__
* `Using the Power BI REST APIs <https://learn.microsoft.com/en-us/rest/api/power-bi/>`__
* `Using the Fabric REST APIs <https://learn.microsoft.com/en-us/rest/api/fabric/articles/using-fabric-apis/>`__
