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


AWS Database Migration Service Operators
========================================

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Overview
--------

Airflow to AWS Database Migration Service (DMS) integration provides several operators to create and interact with
DMS replication tasks.

 - :class:`~airflow.providers.amazon.aws.sensors.dms_task.DmsTaskBaseSensor`
 - :class:`~airflow.providers.amazon.aws.sensors.dms_task.DmsTaskCompletedSensor`
 - :class:`~airflow.providers.amazon.aws.operators.dms_create_task.DmsCreateTaskOperator`
 - :class:`~airflow.providers.amazon.aws.operators.dms_delete_task.DmsDeleteTaskOperator`
 - :class:`~airflow.providers.amazon.aws.operators.dms_describe_tasks.DmsDescribeTasksOperator`
 - :class:`~airflow.providers.amazon.aws.operators.dms_start_task.DmsStartTaskOperator`
 - :class:`~airflow.providers.amazon.aws.operators.dms_stop_task.DmsStopTaskOperator`

One example_dag is provided which showcases some of these operators in action.

 - example_dms_full_load_task.py

.. _howto/operator:DmsCreateTaskOperator:
.. _howto/operator:DmsDeleteTaskOperator:
.. _howto/operator:DmsStartTaskOperator:
.. _howto/sensor:DmsTaskCompletedSensor:

Create replication task, wait for it completion and delete it.
--------------------------------------------------------------

Purpose
"""""""

This example DAG ``example_dms_full_load_task.py`` uses ``DmsCreateTaskOperator``, ``DmsStartTaskOperator``,
``DmsTaskCompletedSensor``, ``DmsDeleteTaskOperator`` to create replication task, start it, wait for it
to be completed, and then delete it.

Defining tasks
""""""""""""""

In the following code we create a new replication task, start it, wait for it to be completed and then delete it.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_dms_full_load_task.py
    :language: python
    :start-after: [START howto_dms_operators]
    :end-before: [END howto_dms_operators]


Reference
---------

For further information, look at:

* `Boto3 Library Documentation for DMS <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dms.html>`__
