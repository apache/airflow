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


.. _howto/operator:AWSDataSyncOperator:

AWS DataSync Operator
=====================

.. contents::
  :depth: 1
  :local:

Overview
--------

Two example_dags are provided which showcase the 
:class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncOperator` 
in action. 

 - example_datasync_simple.py
 - example_datasync_complex.py

Both examples use the :class:`~airflow.providers.amazon.aws.hooks.datasync.AWSDataSyncHook` 
to create a boto3 'datasync' client. This hook in turn uses the :class:`~airflow.contrib.hooks.aws_hook.AwsHook`

Note this guide differentiates between an Airflow task (identified by a task_id on Airflow), 
and an AWS DataSync Task (identified by a TaskArn on AWS)

example_datasync_simple.py
--------------------------

Purpose
"""""""
Find an AWS DataSync TaskArn based on source and destination URIs, and execute it.

Environment variables
"""""""""""""""""""""

Get and Run DataSync Tasks
""""""""""""""""""""""""""

The :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncOperator` can execute a specific
TaskArn by specifying the ``task_arn`` parameter.

.. exampleinclude:: ../../../../../airflow/providers/amazon/aws/example_dags/example_datasync_simple.py
    :language: python
    :start-after: [START howto_operator_datasync_simple_args_1]
    :end-before: [END howto_operator_datasync_simple_args_1]


.. exampleinclude:: ../../../../../airflow/providers/amazon/aws/example_dags/example_datasync_simple.py
    :language: python
    :start-after: [START howto_operator_datasync_simple_1]
    :end-before: [END howto_operator_datasync_simple_1]

Or, it can search in AWS DataSync for a Task based on the specified 
``source_location_uri`` and ``destination_location_uri``. 

In AWS, DataSync Tasks are linked to source and destination Locations. The operator can 
iterate all DataSync Tasks for their source and destination LocationArns. Then it checks
each LocationArn to see if its the URIs match the specified source / destination URI.
Any and all DataSync Tasks that match are returned in a Python list.

.. exampleinclude:: ../../../../../airflow/providers/amazon/aws/example_dags/example_datasync_simple.py
    :language: python
    :start-after: [START howto_operator_datasync_simple_args_2]
    :end-before: [END howto_operator_datasync_simple_args_2]

.. exampleinclude:: ../../../../../airflow/providers/amazon/aws/example_dags/example_datasync_simple.py
    :language: python
    :start-after: [START howto_operator_datasync_simple_2]
    :end-before: [END howto_operator_datasync_simple_2]

Note that there might not always be exactly one TaskArn in AWS and you may need additional logic
handle other cases (see example_datasync_complex).


When an AWS DataSync Task is executed it creates an AWS DataSync TaskExecution. Both the TaskArn 
and the TaskExecutionArn are returned from the operator (and pushed to
an XCom if ``do_xcom_push=True``).

example_datasync_complex.py
---------------------------

Purpose
"""""""

Find and update a DataSync Task, or create one if it doesn't exist. Update the Task, then execute it.
Finally, delete it.


Environment variables
"""""""""""""""""""""

This example relies on the following variables, which can be passed via OS environment variables.

.. exampleinclude:: ../../../../../airflow/providers/amazon/aws/example_dags/example_datasync_complex.py
    :language: python
    :start-after: [START howto_operator_datasync_complex_args]
    :end-before: [END howto_operator_datasync_complex_args]

Get, Create, Update, Run and Delete DataSync Tasks
""""""""""""""""""""""""""""""""""""""""""""""""""

The :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncOperator` is used 
as before but with some extra arguments.

.. exampleinclude:: ../../../../../airflow/providers/amazon/aws/example_dags/example_datasync_complex.py
    :language: python
    :start-after: [START howto_operator_datasync_complex]
    :end-before: [END howto_operator_datasync_complex]

The operator may find 0, 1, or many AWS DataSync Tasks with a matching ``source_location_uri`` and 
``destination_location_uri``.  The operator uses the default behavior to decide what to do in each of 
these scenarios, but you could also override this using your own callable. The description below explains 
the default behavior.

If there were 0 suitable AWS DataSync Tasks found, the operator will try to create one.
This operator will use existing Locations if they match the source or destination location uri
that was specified, or it will attempt to create new Location/s if suitable kwargs were
provided to do so.

If there was 1 AWS DataSync Task found, it will be used.
Lastly, if there were more than 1 AWS DataSync Tasks found, the operator will raise an Exception.

A DataSync Task has therefore either been created, or an existing one was choosen and updated. 

Before starting the AWS DataSync Task, it will be updated with the specified ``update_task_kwargs``, if any.

Next we want to execute our DataSync Task. The operator will create a TaskExecution on AWS DataSync and monitor it to completion.

Finally, because we specified ``delete_task_after_execution=True``, the TaskArn will be deleted from AWS DataSync.


Reference
---------

For further information, look at:

* `AWS boto3 Library Documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/datasync.html>`__
