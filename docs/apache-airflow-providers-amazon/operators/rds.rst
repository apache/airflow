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

======================================================
Amazon Relational Database Service Documentation (RDS)
======================================================

`Amazon Relational Database Service (Amazon RDS) <https://aws.amazon.com/rds/>`__ is a web service that makes it
easier to set up, operate, and scale a relational database in the cloud.
It provides cost-efficient, resizable capacity for an industry-standard relational database and manages
common database administration tasks.

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:RDSCreateDBSnapshotOperator:

Create a database snapshot
==========================

To create a snapshot of an Amazon RDS database instance or cluster you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSCreateDBSnapshotOperator`.
The source database instance must be in the ``available`` or ``storage-optimization`` state.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_rds_snapshot.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_rds_create_db_snapshot]
    :end-before: [END howto_operator_rds_create_db_snapshot]

.. _howto/operator:RDSCopyDBSnapshotOperator:

Copy a database snapshot
========================

To copy a snapshot of an Amazon RDS database instance or cluster you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSCopyDBSnapshotOperator`.
The source database snapshot must be in the ``available`` state.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_rds_snapshot.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_rds_copy_snapshot]
    :end-before: [END howto_operator_rds_copy_snapshot]

.. _howto/operator:RDSDeleteDBSnapshotOperator:

Delete a database snapshot
==========================

To delete a snapshot of an Amazon RDS database instance or cluster you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSDeleteDBSnapshotOperator`.
The database snapshot must be in the ``available`` state to be deleted.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_rds_snapshot.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_rds_delete_snapshot]
    :end-before: [END howto_operator_rds_delete_snapshot]

.. _howto/operator:RDSStartExportTaskOperator:

Export an Amazon RDS snapshot to Amazon S3
==========================================

To export an Amazon RDS snapshot to Amazon S3 you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSStartExportTaskOperator`.
The provided IAM role must have access to the S3 bucket.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_rds_export.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_rds_start_export_task]
    :end-before: [END howto_operator_rds_start_export_task]

.. _howto/operator:RDSCancelExportTaskOperator:

Cancel an Amazon RDS export task
================================

To cancel an Amazon RDS export task to S3 you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSCancelExportTaskOperator`.
Any data that has already been written to the S3 bucket isn't removed.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_rds_export.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_rds_cancel_export]
    :end-before: [END howto_operator_rds_cancel_export]

.. _howto/operator:RDSCreateEventSubscriptionOperator:

Subscribe to an Amazon RDS event notification
=============================================

To create an Amazon RDS event subscription you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSCreateEventSubscriptionOperator`.
This action requires an Amazon SNS topic Amazon Resource Name (ARN).
Amazon RDS event notification is only available for not encrypted SNS topics.
If you specify an encrypted SNS topic, event notifications are not sent for the topic.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_rds_event.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_rds_create_event_subscription]
    :end-before: [END howto_operator_rds_create_event_subscription]

.. _howto/operator:RDSDeleteEventSubscriptionOperator:

Unsubscribe to an Amazon RDS event notification
===============================================

To delete an Amazon RDS event subscription you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSDeleteEventSubscriptionOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_rds_event.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_rds_delete_event_subscription]
    :end-before: [END howto_operator_rds_delete_event_subscription]

.. _howto/operator:RdsCreateDbInstanceOperator:

Create a database instance
==========================

To create a AWS DB instance you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RdsCreateDbInstanceOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_rds_instance.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_rds_create_db_instance]
    :end-before: [END howto_operator_rds_create_db_instance]

.. _howto/operator:RDSDeleteDbInstanceOperator:

Delete a database instance
==========================

To delete a AWS DB instance you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSDeleteDbInstanceOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_rds_instance.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_rds_delete_db_instance]
    :end-before: [END howto_operator_rds_delete_db_instance]

Sensors
-------

.. _howto/sensor:RdsDbSensor:

Wait on an Amazon RDS instance or cluster status
================================================

To wait for an Amazon RDS instance or cluster to reach a specific status you can use
:class:`~airflow.providers.amazon.aws.sensors.rds.RdsDbSensor`.
By default, the sensor waits for a database instance to reach the ``available`` state.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_rds_instance.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_rds_instance]
    :end-before: [END howto_sensor_rds_instance]


.. _howto/sensor:RdsSnapshotExistenceSensor:

Wait on an Amazon RDS snapshot status
=====================================

To wait for an Amazon RDS snapshot with specific statuses you can use
:class:`~airflow.providers.amazon.aws.sensors.rds.RdsSnapshotExistenceSensor`.
By default, the sensor waits for the existence of a snapshot with status ``available``.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_rds_snapshot.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_rds_snapshot_existence]
    :end-before: [END howto_sensor_rds_snapshot_existence]


.. _howto/sensor:RdsExportTaskExistenceSensor:

Wait on an Amazon RDS export task status
========================================

To wait a for an Amazon RDS snapshot export task with specific statuses you can use
:class:`~airflow.providers.amazon.aws.sensors.rds.RdsExportTaskExistenceSensor`.
By default, the sensor waits for the existence of a snapshot with status ``available``.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_rds_export.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_rds_export_task_existence]
    :end-before: [END howto_sensor_rds_export_task_existence]

Reference
---------

* `AWS boto3 library documentation for RDS <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html>`__
* `RDS DB instance statuses <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/accessing-monitoring.html>`__
