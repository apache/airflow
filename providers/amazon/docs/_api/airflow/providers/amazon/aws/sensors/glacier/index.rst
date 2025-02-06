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

:py:mod:`airflow.providers.amazon.aws.sensors.glacier`
======================================================

.. py:module:: airflow.providers.amazon.aws.sensors.glacier


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.glacier.JobStatus
   airflow.providers.amazon.aws.sensors.glacier.GlacierJobOperationSensor




.. py:class:: JobStatus


   Bases: :py:obj:`enum.Enum`

   Glacier jobs description.

   .. py:attribute:: IN_PROGRESS
      :value: 'InProgress'



   .. py:attribute:: SUCCEEDED
      :value: 'Succeeded'




.. py:class:: GlacierJobOperationSensor(*, aws_conn_id = 'aws_default', vault_name, job_id, poke_interval = 60 * 20, mode = 'reschedule', **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Glacier sensor for checking job state. This operator runs only in reschedule mode.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:GlacierJobOperationSensor`

   :param aws_conn_id: The reference to the AWS connection details
   :param vault_name: name of Glacier vault on which job is executed
   :param job_id: the job ID was returned by retrieve_inventory()
   :param poke_interval: Time in seconds that the job should wait in
       between each tries
   :param mode: How the sensor operates.
       Options are: ``{ poke | reschedule }``, default is ``poke``.
       When set to ``poke`` the sensor is taking up a worker slot for its
       whole execution time and sleeps between pokes. Use this mode if the
       expected runtime of the sensor is short or if a short poke interval
       is required. Note that the sensor will hold onto a worker slot and
       a pool slot for the duration of the sensor's runtime in this mode.
       When set to ``reschedule`` the sensor task frees the worker slot when
       the criteria is not yet met and it's rescheduled at a later time. Use
       this mode if the time before the criteria is met is expected to be
       quite long. The poke interval should be more than one minute to
       prevent too much load on the scheduler.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('vault_name', 'job_id')



   .. py:method:: hook()


   .. py:method:: poke(context)

      Override when deriving this class.
