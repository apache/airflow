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


====================================
AWS Database Migration Service (DMS)
====================================

`AWS Database Migration Service (AWS DMS) <https://docs.aws.amazon.com/dms/>`__
is a web service you can use to migrate data from your database that is
on-premises, on an Amazon Relational Database Service (Amazon RDS) DB instance,
or in a database on an Amazon Elastic Compute Cloud (Amazon EC2) instance to a
database on an AWS service.  These services can include a database on Amazon RDS
or a database on an Amazon EC2 instance. You can also migrate a database from an
AWS service to an on-premises database. You can migrate between source and target
endpoints that use the same database engine, such as from an Oracle database to an
Oracle database. You can also migrate between source and target endpoints that use
different database engines, such as from an Oracle database to a PostgreSQL database.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:DmsCreateTaskOperator:

Create a replication task
=========================

To create a replication task you can use
:class:`~airflow.providers.amazon.aws.operators.dms.DmsCreateTaskOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_dms.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dms_create_task]
    :end-before: [END howto_operator_dms_create_task]

.. _howto/operator:DmsStartTaskOperator:

Start a replication task
========================

To start a replication task you can use
:class:`~airflow.providers.amazon.aws.operators.dms.DmsStartTaskOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_dms.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dms_start_task]
    :end-before: [END howto_operator_dms_start_task]

.. _howto/operator:DmsDescribeTasksOperator:

Get details of replication tasks
================================

To retrieve the details for a list of replication tasks you can use
:class:`~airflow.providers.amazon.aws.operators.dms.DmsDescribeTasksOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_dms.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dms_describe_tasks]
    :end-before: [END howto_operator_dms_describe_tasks]

.. _howto/operator:DmsStopTaskOperator:

Stop a replication task
=======================

To stop a replication task you can use
:class:`~airflow.providers.amazon.aws.operators.dms.DmsStopTaskOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_dms.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dms_stop_task]
    :end-before: [END howto_operator_dms_stop_task]

.. _howto/operator:DmsDeleteTaskOperator:

Delete a replication task
=========================

To delete a replication task you can use
:class:`~airflow.providers.amazon.aws.operators.dms.DmsDeleteTaskOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_dms.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dms_delete_task]
    :end-before: [END howto_operator_dms_delete_task]

Sensors
-------

.. _howto/sensor:DmsTaskCompletedSensor:

Wait for a replication task to complete
=======================================

To check the state of a replication task until it is completed, you can use
:class:`~airflow.providers.amazon.aws.sensors.dms.DmsTaskCompletedSensor`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_dms.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_dms_task_completed]
    :end-before: [END howto_sensor_dms_task_completed]


Reference
---------

* `AWS boto3 library documentation for DMS <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dms.html>`__
