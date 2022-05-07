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

RDS management operators
=====================================

.. contents::
  :depth: 1
  :local:


.. _howto/operator:RDSCreateDBSnapshotOperator:

Create DB snapshot
""""""""""""""""""

To create a snapshot of AWS RDS DB instance or DB cluster snapshot you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSCreateDBSnapshotOperator`.
The source DB instance must be in the ``available`` or ``storage-optimization`` state.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_rds.py
    :language: python
    :start-after: [START rds_snapshots_howto_guide]
    :end-before: [END rds_snapshots_howto_guide]


This Operator leverages the AWS CLI
`create-db-snapshot <https://docs.aws.amazon.com/cli/latest/reference/rds/create-db-snapshot.html>`__ API
`create-db-cluster-snapshot <https://docs.aws.amazon.com/cli/latest/reference/rds/create-db-cluster-snapshot.html>`__ API


.. _howto/operator:RDSCopyDBSnapshotOperator:

Copy DB snapshot
""""""""""""""""

To copy AWS RDS DB instance or DB cluster snapshot you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSCopyDBSnapshotOperator`.
The source DB snapshot must be in the ``available`` state.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_rds.py
    :language: python
    :start-after: [START howto_guide_rds_copy_snapshot]
    :end-before: [END howto_guide_rds_copy_snapshot]

This Operator leverages the AWS CLI
`copy-db-snapshot <https://docs.aws.amazon.com/cli/latest/reference/rds/copy-db-snapshot.html>`__ API
`copy-db-cluster-snapshot <https://docs.aws.amazon.com/cli/latest/reference/rds/copy-db-cluster-snapshot.html>`__ API


.. _howto/operator:RDSDeleteDBSnapshotOperator:

Delete DB snapshot
""""""""""""""""""

To delete AWS RDS DB instance or DB cluster snapshot you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSDeleteDBSnapshotOperator`.
The DB snapshot must be in the ``available`` state to be deleted.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_rds.py
    :language: python
    :start-after: [START howto_guide_rds_delete_snapshot]
    :end-before: [END howto_guide_rds_delete_snapshot]

This Operator leverages the AWS CLI
`delete-db-snapshot <https://docs.aws.amazon.com/cli/latest/reference/rds/delete-db-snapshot.html>`__ API
`delete-db-cluster-snapshot <https://docs.aws.amazon.com/cli/latest/reference/rds/delete-db-cluster-snapshot.html>`__ API


.. _howto/operator:RDSStartExportTaskOperator:

Start export task
"""""""""""""""""

To start task that exports RDS snapshot to S3 you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSStartExportTaskOperator`.
The provided IAM role must have access to the S3 bucket.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_rds.py
    :language: python
    :start-after: [START howto_guide_rds_start_export]
    :end-before: [END howto_guide_rds_start_export]

This Operator leverages the AWS CLI
`start-export-task <https://docs.aws.amazon.com/cli/latest/reference/rds/start-export-task.html>`__ API


.. _howto/operator:RDSCancelExportTaskOperator:

Cancel export task
""""""""""""""""""

To cancel task that exports RDS snapshot to S3 you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSCancelExportTaskOperator`.
Any data that has already been written to the S3 bucket isn't removed.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_rds.py
    :language: python
    :start-after: [START howto_guide_rds_cancel_export]
    :end-before: [END howto_guide_rds_cancel_export]

This Operator leverages the AWS CLI
`cancel-export-task <https://docs.aws.amazon.com/cli/latest/reference/rds/cancel-export-task.html>`__ API


.. _howto/operator:RDSCreateEventSubscriptionOperator:

Create event subscription
"""""""""""""""""""""""""

To create event subscription you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSCreateEventSubscriptionOperator`.
This action requires a topic Amazon Resource Name (ARN) created by either the RDS console, the SNS console, or the SNS API.
To obtain an ARN with SNS, you must create a topic in Amazon SNS and subscribe to the topic.
RDS event notification is only available for not encrypted SNS topics.
If you specify an encrypted SNS topic, event notifications are not sent for the topic.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_rds.py
    :language: python
    :start-after: [START howto_guide_rds_create_subscription]
    :end-before: [END howto_guide_rds_create_subscription]

This Operator leverages the AWS CLI
`create-event-subscription <https://docs.aws.amazon.com/cli/latest/reference/rds/create-event-subscription.html>`__ API


.. _howto/operator:RDSDeleteEventSubscriptionOperator:

Delete event subscription
"""""""""""""""""""""""""

To delete event subscription you can use
:class:`~airflow.providers.amazon.aws.operators.rds.RDSDeleteEventSubscriptionOperator`

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_rds.py
    :language: python
    :start-after: [START howto_guide_rds_delete_subscription]
    :end-before: [END howto_guide_rds_delete_subscription]

This Operator leverages the AWS CLI
`delete-event-subscription <https://docs.aws.amazon.com/cli/latest/reference/rds/delete-event-subscription.html>`__ API


RDS management sensors
=====================================

.. contents::
  :depth: 1
  :local:


.. _howto/operator:RdsSnapshotExistenceSensor:

DB snapshot sensor
""""""""""""""""""

To wait a snapshot with certain statuses of AWS RDS DB instance or DB cluster snapshot you can use
:class:`~airflow.providers.amazon.aws.sensors.rds.RdsSnapshotExistenceSensor`.
By default, sensor waits existence of snapshot with status ``available``.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_rds.py
    :language: python
    :start-after: [START howto_guide_rds_snapshot_sensor]
    :end-before: [END howto_guide_rds_snapshot_sensor]


.. _howto/operator:RdsExportTaskExistenceSensor:

Export task sensor
""""""""""""""""""

To wait a snapshot export task with certain statuses you can use
:class:`~airflow.providers.amazon.aws.sensors.rds.RdsExportTaskExistenceSensor`.
By default, sensor waits existence of export task with status ``available``.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_rds.py
    :language: python
    :start-after: [START howto_guide_rds_export_sensor]
    :end-before: [END howto_guide_rds_export_sensor]

Reference
---------

For further information, look at:

* `Boto3 Library Documentation for RDS <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html>`__
