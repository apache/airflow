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

Google Dataplex Operators
=========================

Dataplex is an intelligent data fabric that provides unified analytics
and data management across your data lakes, data warehouses, and data marts.

For more information about the task visit `Dataplex production documentation <Product documentation <https://cloud.google.com/dataplex/docs/reference>`__

Create a Task
-------------

Before you create a dataplex task you need to define its body.
For more information about the available fields to pass when creating a task, visit `Dataplex create task API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.lakes.tasks#Task>`__

A simple task configuration can look as followed:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_configuration]
    :end-before: [END howto_dataplex_configuration]

With this configuration we can create the task both synchronously & asynchronously:
:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateTaskOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_task_operator]
    :end-before: [END howto_dataplex_create_task_operator]

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_async_create_task_operator]
    :end-before: [END howto_dataplex_async_create_task_operator]

Delete a task
-------------

To delete a task you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteTaskOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_task_operator]
    :end-before: [END howto_dataplex_delete_task_operator]

List tasks
----------

To list tasks you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexListTasksOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_list_tasks_operator]
    :end-before: [END howto_dataplex_list_tasks_operator]

Get a task
----------

To get a task you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexGetTaskOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_task_operator]
    :end-before: [END howto_dataplex_get_task_operator]

Wait for a task
---------------

To wait for a task created asynchronously you can use:

:class:`~airflow.providers.google.cloud.sensors.dataplex.DataplexTaskStateSensor`

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_task_state_sensor]
    :end-before: [END howto_dataplex_task_state_sensor]

Create a Lake
-------------

Before you create a dataplex lake you need to define its body.

For more information about the available fields to pass when creating a lake, visit `Dataplex create lake API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.lakes#Lake>`__

A simple task configuration can look as followed:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_lake_configuration]
    :end-before: [END howto_dataplex_lake_configuration]

With this configuration we can create the lake:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateLakeOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_lake_operator]
    :end-before: [END howto_dataplex_create_lake_operator]


Delete a lake
-------------

To delete a lake you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteLakeOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_lake_operator]
    :end-before: [END howto_dataplex_delete_lake_operator]
