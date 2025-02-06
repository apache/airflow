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

:py:mod:`airflow.providers.amazon.aws.sensors.rds`
==================================================

.. py:module:: airflow.providers.amazon.aws.sensors.rds


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.rds.RdsSnapshotExistenceSensor
   airflow.providers.amazon.aws.sensors.rds.RdsExportTaskExistenceSensor
   airflow.providers.amazon.aws.sensors.rds.RdsDbSensor




.. py:class:: RdsSnapshotExistenceSensor(*, db_type, db_snapshot_identifier, target_statuses = None, aws_conn_id = 'aws_conn_id', **kwargs)


   Bases: :py:obj:`RdsBaseSensor`

   Waits for RDS snapshot with a specific status.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:RdsSnapshotExistenceSensor`

   :param db_type: Type of the DB - either "instance" or "cluster"
   :param db_snapshot_identifier: The identifier for the DB snapshot
   :param target_statuses: Target status of snapshot

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('db_snapshot_identifier', 'target_statuses')



   .. py:method:: poke(context)

      Override when deriving this class.



.. py:class:: RdsExportTaskExistenceSensor(*, export_task_identifier, target_statuses = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`RdsBaseSensor`

   Waits for RDS export task with a specific status.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:RdsExportTaskExistenceSensor`

   :param export_task_identifier: A unique identifier for the snapshot export task.
   :param target_statuses: Target status of export task

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('export_task_identifier', 'target_statuses')



   .. py:method:: poke(context)

      Override when deriving this class.



.. py:class:: RdsDbSensor(*, db_identifier, db_type = RdsDbType.INSTANCE, target_statuses = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`RdsBaseSensor`

   Waits for an RDS instance or cluster to enter one of a number of states.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:RdsDbSensor`

   :param db_type: Type of the DB - either "instance" or "cluster" (default: 'instance')
   :param db_identifier: The AWS identifier for the DB
   :param target_statuses: Target status of DB

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('db_identifier', 'db_type', 'target_statuses')



   .. py:method:: poke(context)

      Override when deriving this class.
