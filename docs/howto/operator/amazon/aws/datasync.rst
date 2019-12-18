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

 - example_datasync_1.py
 - example_datasync_2.py

Both examples use the :class:`~airflow.providers.amazon.aws.hooks.datasync.AWSDataSyncHook` 
to create a boto3 DataSync client. This hook in turn uses the :class:`~airflow.contrib.hooks.aws_hook.AwsHook`

Note this guide differentiates between an *Airflow task* (identified by a task_id on Airflow), 
and an *AWS DataSync Task* (identified by a TaskArn on AWS).

example_datasync_1.py
--------------------------

Purpose
"""""""
With this DAG we show approaches catering for two simple use cases.

1.1 Specify a TaskARN to be executed.
1.2 Find an AWS DataSync TaskArn based on source and destination URIs, and execute it.

Environment variables
"""""""""""""""""""""

These examples rely on the following variables, which can be passed via OS environment variables.

.. exampleinclude:: ../../../../../airflow/providers/amazon/aws/example_dags/example_datasync_1.py
    :language: python
    :start-after: [START howto_operator_datasync_1_args_1]
    :end-before: [END howto_operator_datasync_1_args_1]

.. exampleinclude:: ../../../../../airflow/providers/amazon/aws/example_dags/example_datasync_1.py
    :language: python
    :start-after: [START howto_operator_datasync_1_args_2]
    :end-before: [END howto_operator_datasync_1_args_2]

Get DataSync Tasks
""""""""""""""""""

The :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncOperator` can execute a specific
TaskArn by specifying the ``task_arn`` parameter. This is useful when you know the TaskArn you want to execute.

.. exampleinclude:: ../../../../../airflow/providers/amazon/aws/example_dags/example_datasync_1.py
    :language: python
    :start-after: [START howto_operator_datasync_1_1]
    :end-before: [END howto_operator_datasync_1_1]

Alternatively, the operator can search in AWS DataSync for a Task based on 
``source_location_uri`` and ``destination_location_uri``. For example, your 
``source_location_uri`` might point to your on-premises SMB / NFS share, and your 
``destination_location_uri`` might be an S3 bucket.

In AWS, DataSync Tasks are linked to source and destination Locations. A location has a LocationURI and
is referenced by a LocationArn much like other AWS resources.
The :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncOperator`
can iterate all DataSync Tasks for their source and destination LocationArns. Then it checks
each LocationArn to see if its the URIs match the desired source / destination URI.

To perform a search based on the Location URIs, define the task as follows

.. exampleinclude:: ../../../../../airflow/providers/amazon/aws/example_dags/example_datasync_1.py
    :language: python
    :start-after: [START howto_operator_datasync_1_2]
    :end-before: [END howto_operator_datasync_1_2]

Note: The above configuration assumes there is always exactly one DataSync TaskArn in AWS that matches.
It will fail if either there were no matching TaskArns or if there were more than one matching TaskArn
defined already in AWS DataSync. You may want to add additional logic to handle other cases 
- see example_datasync_2 and the `Operator behaviour`_ section.

example_datasync_2.py
---------------------

Purpose
"""""""

Show how DataSync Tasks and Locations can be automatically created, deleted and updated using the 
:class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncOperator`.

Find and update a DataSync Task, or create one if it doesn't exist. Update the Task, then execute it.
Finally, delete it.

Environment variables
"""""""""""""""""""""

This example relies on the following variables, which can be passed via OS environment variables.

.. exampleinclude:: ../../../../../airflow/providers/amazon/aws/example_dags/example_datasync_2.py
    :language: python
    :start-after: [START howto_operator_datasync_2_args]
    :end-before: [END howto_operator_datasync_2_args]

Get, Create, Update, Run and Delete DataSync Tasks
""""""""""""""""""""""""""""""""""""""""""""""""""

The :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncOperator` is used 
as before but with some extra arguments.

Most of the arguments (``CREATE_*_KWARGS``) provide a way for the operator to automatically create a Task
and/or Locations if no suitable existing Task was found. If these are left to their default value (None)
then no create will be attempted.

.. exampleinclude:: ../../../../../airflow/providers/amazon/aws/example_dags/example_datasync_2.py
    :language: python
    :start-after: [START howto_operator_datasync_2]
    :end-before: [END howto_operator_datasync_2]

Note also the addition of ``UPDATE_TASK_KWARGS``; if this is not None then it will be used to do an
update of the Task properties on AWS prior to the Task being executed.

Otherwise the behaviour is very similar to the first examples above. We want to identify a suitable TaskArn
based on some criteria (specified task_arn or source and dest URIs) and execute it. In this example,
the main differences are that we provide a way to create Tasks/Locations if none are found.

Also, because we specified ``delete_task_after_execution=True``, the TaskArn will be deleted 
from AWS DataSync after it completes successfully.

Operator behaviour
------------------

DataSync Task execution behaviour
"""""""""""""""""""""""""""""""""

Once the :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncOperator` has identified
the correct TaskArn to run (either because you specified it, or because it was found), it will then be 
executed. Whenever an AWS DataSync Task is executed it creates an AWS DataSync TaskExecution, identified
by a TaskExecutionArn. 

The TaskExecutionArn will be monitored until completion (success / failure), and its status will be 
periodically written to the Airflow task log.

After completion, the TaskExecution description is retrieved from AWS and dumped to the Airflow task log
for inspection.

Finally, both the TaskArn and the TaskExecutionArn are returned from the ``execute()`` method, and pushed to
an XCom automatically if ``do_xcom_push=True``.

The :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncOperator` supports
optional passing of additional kwargs to the underlying ``boto3.start_task_execution()`` API.
This is done with the ``task_execution_kwargs`` parameter.
This is useful for example to limit bandwidth or filter included files - refer to the boto3 Datasync
documentation for more details.

TaskArn selection behaviour
"""""""""""""""""""""""""""

The :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncOperator`
may find 0, 1, or many AWS DataSync Tasks with a matching ``source_location_uri`` and 
``destination_location_uri``. The operator must decide what to do in each of these scenarios.

To override the default behaviour, simply create an operator which inherits 
:class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncOperator`
and re-implement the ``choose_task`` and ``choose_location`` methods
to suit your use case.

Scenarios and behaviours:

 - No suitable AWS DataSync Tasks found

If there were 0 suitable AWS DataSync Tasks found, the operator will try to create one.
This operator will use existing Locations if any are found which match the source or destination
location uri that were specified. Or, if either location has no matching LocationArn in AWS then
the operator will attempt to create new Location/s if suitable kwargs were provided to do so.

 - 1 AWS DataSync Task found

This is the simplest scenario - just use the one DataSync Task that was found :).

 - More than one AWS DataSync Tasks found

The operator will raise an Exception. To avoid this, you can set ``allow_random_task_choice=True``
to randomly choose from candidate Tasks. Alternatively you can subclass this operator
and re-implement the ``choose_task`` method with your own algorithm.

TaskArn creation behaviour
"""""""""""""""""""""""""""

When creating a Task, the
:class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncOperator` will try to find
and use existing LocationArns rather than creating new ones. If multiple LocationArns match the
specied URIs then we need to choose one to use. In this scenario, the operator behaves similarly
to how it chooses a single Task from many Tasks:

The operator will raise an Exception. To avoid this, you can set ``allow_random_location_choice=True``
to randomly choose from candidate Locations. Alternatively you can subclass this operator
and re-implement the ``choose_location`` method with your own algorithm.


Reference
---------

For further information, look at:

* `AWS boto3 Library Documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/datasync.html>`__
