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



AWS DataSync Operators
===============================

.. contents::
  :depth: 1
  :local:

Overview
--------

Two example_dags are provided which showcase AWS DataSync operators in action. 
This guide will describe them in order.

 - example_datasync_simple.py
 - example_datasync_complex.py

All the datasync operators use the :class:`~airflow.providers.amazon.aws.hooks.datasync.AWSDataSyncHook` 
to create a boto3 'datasync' client. This hook in turn uses the :class:`~airflow.contrib.hooks.aws_hook.AwsHook`

Note this guide differentiates between an Airflow task (identified by a task_id on Airflow), 
and an AWS DataSync Task (identified by a TaskArn on AWS)

example_datasync_simple.py
--------------------------

Purpose
"""""""
Find an AWS DataSync TaskArn based on source and destination URIs, and execute it.

The following operators are showcased:
 * :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncGetTasksOperator`
 * :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncTaskOperator`

Environment variables
"""""""""""""""""""""

This example relies on the following variables, which can be passed via OS environment variables.

.. exampleinclude:: ../../../../airflow/providers/amazon/aws/example_dags/example_datasync_simple.py
    :language: python
    :start-after: [START howto_operator_datasync_simple_args]
    :end-before: [END howto_operator_datasync_simple_args]

.. _howto/operator:AWSDataSyncGetTasksOperator:
.. _howto/operator:AWSDataSyncTaskOperator:

Get and Run DataSync Tasks
""""""""""""""""""""""""""

The :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncGetTasksOperator` searches 
in AWS DataSync for a Task based on the specified ``source_location_uri`` and ``destination_location_uri``.

In AWS, DataSync Tasks are linked to source and destination Locations. This operator will 
iterate all DataSync Taskss for their source and destination LocationArns. Then it checks
each LocationArn to see if its the URIs match the specified source / destination URI.
Any and all DataSync Tasks that match are returned in a Python list.

The return value is automatically pushed to an XCom which you can view in the Airflow frontend.

.. exampleinclude:: ../../../../airflow/providers/amazon/aws/example_dags/example_datasync_simple.py
    :language: python
    :start-after: [START howto_operator_datasync_simple_get_tasks]
    :end-before: [END howto_operator_datasync_simple_get_tasks]

The :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncTaskOperator` executes
a specified AWS DataSync Task identified by the ``task_arn`` parameter. 
We assume that the previous Airflow task managed to retrieve exactly one TaskArn.
We use an ``xcom_pull`` to retrieve that TaskArn and pass it to this Airflow task.

Note that there might not always be exactly one TaskArn returned and you may need additional logic
handle other cases (see example_datasync_complex).

When an AWS DataSync Task is executed it creates an AWS DataSync TaskExecution.
The TaskExecutionArn is returned from this operator and automaticaly pushed to an XCom.

.. exampleinclude:: ../../../../airflow/providers/amazon/aws/example_dags/example_datasync_simple.py
    :language: python
    :start-after: [START howto_operator_datasync_simple_run_task]
    :end-before: [END howto_operator_datasync_simple_run_task]

We have now defined Airflow tasks to get a DataSync Task and execute it.
Airflow task dependencies are strightforward.

.. exampleinclude:: ../../../../airflow/providers/amazon/aws/example_dags/example_datasync_simple.py
    :language: python
    :start-after: [START howto_operator_datasync_simple_dependencies]
    :end-before: [END howto_operator_datasync_simple_dependencies]

example_datasync_complex.py
---------------------------

Purpose
"""""""
Find and update a DataSync Task, or create one if it doesn't exist, and finally execute it.
This DAG uses Airflow XCom to pass messages between the DataSync operators.

The following operators are showcased:
 * :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncGetTasksOperator`
 * :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncUpdateTaskOperator`
 * :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncCreateTaskOperator`
 * :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncTaskOperator`



Environment variables
"""""""""""""""""""""

This example relies on the following variables, which can be passed via OS environment variables.

.. exampleinclude:: ../../../../airflow/providers/amazon/aws/example_dags/example_datasync_complex.py
    :language: python
    :start-after: [START howto_operator_datasync_complex_args]
    :end-before: [END howto_operator_datasync_complex_args]


.. _howto/operator:AWSDataSyncUpdateTaskOperator:
.. _howto/operator:AWSDataSyncCreateTaskOperator:

Get, Create, Update and Run DataSync Tasks
""""""""""""""""""""""""""""""""""""""""""

The :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncGetTasksOperator` is used 
as before.

.. exampleinclude:: ../../../../airflow/providers/amazon/aws/example_dags/example_datasync_complex.py
    :language: python
    :start-after: [START howto_operator_datasync_complex_get_tasks]
    :end-before: [END howto_operator_datasync_complex_get_tasks]

The ``get_task`` may have found 0, 1, or many AWS DataSync Tasks. The ``decide_task`` uses the 
:class:`~airflow.operators.python_operator.BranchPythonOperator` to decide what to do for each
of these scenarios. It uses a Python calleable which returns the Airflow task_id to run next.
All other downstream task_ids will be skipped.

.. exampleinclude:: ../../../../airflow/providers/amazon/aws/example_dags/example_datasync_complex.py
    :language: python
    :start-after: [START howto_operator_datasync_complex_decide_task]
    :end-before: [END howto_operator_datasync_complex_decide_task]

.. exampleinclude:: ../../../../airflow/providers/amazon/aws/example_dags/example_datasync_complex.py
    :language: python
    :start-after: [START howto_operator_datasync_complex_decide_function]
    :end-before: [END howto_operator_datasync_complex_decide_function]

If there were 0 suitable AWS DataSync Tasks found, we create one with the
:class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataCreateTaskOperator`.
This operator will use existing Locations if they match the source or destination location uri
that was specified, or it will attempt to create new Location/s if suitable kwargs were
provided to do so.

.. exampleinclude:: ../../../../airflow/providers/amazon/aws/example_dags/example_datasync_complex.py
    :language: python
    :start-after: [START howto_operator_datasync_complex_create_task]
    :end-before: [END howto_operator_datasync_complex_create_task]

If there was 1 AWS DataSync Task found, it can be updated using the 
:class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataUpdateTaskOperator`.
The AWS DataSync Task will be updated with the specified ``update_task_kwargs``.

.. exampleinclude:: ../../../../airflow/providers/amazon/aws/example_dags/example_datasync_complex.py
    :language: python
    :start-after: [START howto_operator_datasync_complex_update_task]
    :end-before: [END howto_operator_datasync_complex_update_task]

A DataSync Task has either been created, or an existing one was choosen and updated. Next we
want to execute our DataSync Task. Because Airflow workflow was branched previosuly, we first join it up again.
For more information, consider the task dependencies shown at the end of this example.

Normally the :class:`~airflow.operators.python_operator.BranchPythonOperator` skips all downstream 
tasks that were not chosen, so we set ``trigger_rule='none_failed'`` to prevent the skip from
cascading to our ``run_task``. When either the ``create_task`` or ``update_task`` succeeds the ``join_task`` must be
triggered.

.. exampleinclude:: ../../../../airflow/providers/amazon/aws/example_dags/example_datasync_complex.py
    :language: python
    :start-after: [START howto_operator_datasync_complex_join_task]
    :end-before: [END howto_operator_datasync_complex_join_task]

The join function uses the output of the ``decide_task`` to identify which ``task_arn`` we want to run.
We avoid using the return value from ``create_task`` or ``update_task`` to make this decision, because when 
tasks are rerun the XCom push from previously run tasks might still exist.
So we rather check the ``decide_task`` to detemine which of the two return_values to use.

.. exampleinclude:: ../../../../airflow/providers/amazon/aws/example_dags/example_datasync_complex.py
    :language: python
    :start-after: [START howto_operator_datasync_complex_join_function]
    :end-before: [END howto_operator_datasync_complex_join_function]

Finally, we want to run our AWS DataSync Task.

.. exampleinclude:: ../../../../airflow/providers/amazon/aws/example_dags/example_datasync_complex.py
    :language: python
    :start-after: [START howto_operator_datasync_complex_run_task]
    :end-before: [END howto_operator_datasync_complex_run_task]

Task dependencies are a little more complex than before, due to the branching and joining.

.. exampleinclude:: ../../../../airflow/providers/amazon/aws/example_dags/example_datasync_complex.py
    :language: python
    :start-after: [START howto_operator_datasync_complex_dependencies]
    :end-before: [END howto_operator_datasync_complex_dependencies]

Reference
---------

For further information, look at:

* `AWS boto3 Library Documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/datasync.html>`__
