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

============
AWS DataSync
============

`AWS DataSync <https://aws.amazon.com/datasync/>`__ is a data-transfer service that simplifies, automates,
and accelerates moving and replicating data between on-premises storage systems and AWS storage services over
the internet or AWS Direct Connect.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:DataSyncOperator:

Interact with AWS DataSync Tasks
================================

You can use :class:`~airflow.providers.amazon.aws.operators.datasync.DataSyncOperator` to
find, create, update, execute and delete AWS DataSync tasks.

Once the :class:`~airflow.providers.amazon.aws.operators.datasync.DataSyncOperator` has identified
the correct TaskArn to run (either because you specified it, or because it was found), it will then be
executed. Whenever an AWS DataSync Task is executed it creates an AWS DataSync TaskExecution, identified
by a TaskExecutionArn.

The TaskExecutionArn will be monitored until completion (success / failure), and its status will be
periodically written to the Airflow task log.

The :class:`~airflow.providers.amazon.aws.operators.datasync.DataSyncOperator` supports
optional passing of additional kwargs to the underlying ``boto3.start_task_execution()`` API.
This is done with the ``task_execution_kwargs`` parameter.
This is useful for example to limit bandwidth or filter included files, see the `boto3 Datasync
documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/datasync.html>`__
for more details.

Execute a task
""""""""""""""

To execute a specific task, you can pass the ``task_arn`` to the operator.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_datasync.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_datasync_specific_task]
    :end-before: [END howto_operator_datasync_specific_task]

Search and execute a task
"""""""""""""""""""""""""

To search for a task, you can specify the ``source_location_uri`` and ``destination_location_uri`` to the operator.
If one task is found, this one will be executed.
If more than one task is found, the operator will raise an Exception. To avoid this, you can set
``allow_random_task_choice`` to ``True`` to randomly choose from candidate tasks.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_datasync.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_datasync_search_task]
    :end-before: [END howto_operator_datasync_search_task]

Create and execute a task
"""""""""""""""""""""""""

When searching for a task, if no task is found you have the option to create one before executing it.
In order to do that, you need to provide the extra parameters ``create_task_kwargs``, ``create_source_location_kwargs``
and ``create_destination_location_kwargs``.

These extra parameters provide a way for the operator to automatically create a Task and/or Locations if no suitable
existing Task was found. If these are left to their default value (None) then no create will be attempted.

Also, because ``delete_task_after_execution`` is set to ``True``, the task will be deleted
from AWS DataSync after it completes successfully.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_datasync.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_datasync_create_task]
    :end-before: [END howto_operator_datasync_create_task]


When creating a Task, the
:class:`~airflow.providers.amazon.aws.operators.datasync.DataSyncOperator` will try to find
and use existing LocationArns rather than creating new ones. If multiple LocationArns match the
specified URIs then we need to choose one to use. In this scenario, the operator behaves similarly
to how it chooses a single Task from many Tasks:

The operator will raise an Exception. To avoid this, you can set ``allow_random_location_choice`` to ``True``
to randomly choose from candidate Locations.

Reference
---------

* `AWS boto3 library documentation for DataSync <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/datasync.html>`__
