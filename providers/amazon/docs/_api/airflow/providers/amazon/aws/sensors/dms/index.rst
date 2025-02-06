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

:py:mod:`airflow.providers.amazon.aws.sensors.dms`
==================================================

.. py:module:: airflow.providers.amazon.aws.sensors.dms


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.dms.DmsTaskBaseSensor
   airflow.providers.amazon.aws.sensors.dms.DmsTaskCompletedSensor




.. py:class:: DmsTaskBaseSensor(replication_task_arn, aws_conn_id='aws_default', target_statuses = None, termination_statuses = None, *args, **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Contains general sensor behavior for DMS task.

   Subclasses should set ``target_statuses`` and ``termination_statuses`` fields.

   :param replication_task_arn: AWS DMS replication task ARN
   :param aws_conn_id: aws connection to uses
   :param target_statuses: the target statuses, sensor waits until
       the task reaches any of these states
   :param termination_statuses: the termination statuses, sensor fails when
       the task reaches any of these states

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('replication_task_arn',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:method:: get_hook()

      Get DmsHook.


   .. py:method:: hook()


   .. py:method:: poke(context)

      Override when deriving this class.



.. py:class:: DmsTaskCompletedSensor(*args, **kwargs)


   Bases: :py:obj:`DmsTaskBaseSensor`

   Pokes DMS task until it is completed.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:DmsTaskCompletedSensor`

   :param replication_task_arn: AWS DMS replication task ARN

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('replication_task_arn',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()
