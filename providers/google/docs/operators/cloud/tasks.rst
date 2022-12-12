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

Google Cloud Tasks
==================
Cloud Tasks is a fully managed service that allows you to manage the execution, dispatch,
and delivery of a large number of distributed tasks.
Using Cloud Tasks, you can perform work asynchronously outside of a user or service-to-service request.

For more information about the service visit
`Cloud Tasks product documentation <https://cloud.google.com/tasks/docs>`__

Prerequisite Tasks
------------------

.. include::/operators/_partials/prerequisite_tasks.rst


Queue operations
----------------

.. _howto/operator:CloudTasksQueueCreateOperator:

Create queue
^^^^^^^^^^^^

To create new Queue use
:class:`~airflow.providers.google.cloud.operators.tasks.CloudTasksQueueCreateOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/tasks/example_queue.py
    :language: python
    :dedent: 4
    :start-after: [START create_queue]
    :end-before: [END create_queue]

.. _howto/operator:CloudTasksQueueDeleteOperator:

Delete queue
^^^^^^^^^^^^

To delete Queue use
:class:`~airflow.providers.google.cloud.operators.tasks.CloudTasksQueueDeleteOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/tasks/example_queue.py
    :language: python
    :dedent: 4
    :start-after: [START delete_queue]
    :end-before: [END delete_queue]


.. _howto/operator:CloudTasksQueueResumeOperator:

Resume queue
^^^^^^^^^^^^

To resume Queue use
:class:`~airflow.providers.google.cloud.operators.tasks.CloudTasksQueueResumeOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/tasks/example_queue.py
    :language: python
    :dedent: 4
    :start-after: [START resume_queue]
    :end-before: [END resume_queue]

.. _howto/operator:CloudTasksQueuePauseOperator:

Pause queue
^^^^^^^^^^^

To pause Queue use
:class:`~airflow.providers.google.cloud.operators.tasks.CloudTasksQueuePauseOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/tasks/example_queue.py
    :language: python
    :dedent: 4
    :start-after: [START pause_queue]
    :end-before: [END pause_queue]

.. _howto/operator:CloudTasksQueuePurgeOperator:

Purge queue
^^^^^^^^^^^

To purge Queue use
:class:`~airflow.providers.google.cloud.operators.tasks.CloudTasksQueuePurgeOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/tasks/example_queue.py
    :language: python
    :dedent: 4
    :start-after: [START purge_queue]
    :end-before: [END purge_queue]

.. _howto/operator:CloudTasksQueueGetOperator:

Get queue
^^^^^^^^^

To get Queue use
:class:`~airflow.providers.google.cloud.operators.tasks.CloudTasksQueueGetOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/tasks/example_queue.py
    :language: python
    :dedent: 4
    :start-after: [START get_queue]
    :end-before: [END get_queue]

.. _howto/operator:CloudTasksQueueUpdateOperator:

Update queue
^^^^^^^^^^^^

To update Queue use
:class:`~airflow.providers.google.cloud.operators.tasks.CloudTasksQueueUpdateOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/tasks/example_queue.py
    :language: python
    :dedent: 4
    :start-after: [START update_queue]
    :end-before: [END update_queue]

.. _howto/operator:CloudTasksQueuesListOperator:

List queues
^^^^^^^^^^^

To list all Queues use
:class:`~airflow.providers.google.cloud.operators.tasks.CloudTasksQueuesListOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/tasks/example_queue.py
    :language: python
    :dedent: 4
    :start-after: [START list_queue]
    :end-before: [END list_queue]


Tasks operations
----------------

.. _howto/operator:CloudTasksTaskCreateOperator:

Create task
^^^^^^^^^^^

To create new Task in a particular queue use
:class:`~airflow.providers.google.cloud.operators.tasks.CloudTasksTaskCreateOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/tasks/example_tasks.py
    :language: python
    :dedent: 4
    :start-after: [START create_task]
    :end-before: [END create_task]

.. _howto/operator:CloudTasksTaskGetOperator:

Get task
^^^^^^^^

To get the Tasks in a particular queue use
:class:`~airflow.providers.google.cloud.operators.tasks.CloudTasksTaskGetOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/tasks/example_tasks.py
    :language: python
    :dedent: 4
    :start-after: [START tasks_get]
    :end-before: [END tasks_get]

.. _howto/operator:CloudTasksTaskRunOperator:

Run task
^^^^^^^^

To run the Task in a particular queue use
:class:`~airflow.providers.google.cloud.operators.tasks.CloudTasksTaskRunOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/tasks/example_tasks.py
    :language: python
    :dedent: 4
    :start-after: [START run_task]
    :end-before: [END run_task]

.. _howto/operator:CloudTasksTasksListOperator:

List tasks
^^^^^^^^^^

To list all Tasks in a particular queue use
:class:`~airflow.providers.google.cloud.operators.tasks.CloudTasksTasksListOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/tasks/example_tasks.py
    :language: python
    :dedent: 4
    :start-after: [START list_tasks]
    :end-before: [END list_tasks]

.. _howto/operator:CloudTasksTaskDeleteOperator:

Delete task
^^^^^^^^^^^

To delete the Task from particular queue use
:class:`~airflow.providers.google.cloud.operators.tasks.CloudTasksTaskDeleteOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/tasks/example_tasks.py
    :language: python
    :dedent: 4
    :start-after: [START create_task]
    :end-before: [END create_task]


References
^^^^^^^^^^
For further information, take a look at:

* `Cloud Tasks API documentation <https://cloud.google.com/tasks/docs/reference/rest>`__
* `Product documentation <https://cloud.google.com/tasks/docs>`__
