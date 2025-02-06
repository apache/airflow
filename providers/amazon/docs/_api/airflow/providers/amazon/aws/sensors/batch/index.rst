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

:py:mod:`airflow.providers.amazon.aws.sensors.batch`
====================================================

.. py:module:: airflow.providers.amazon.aws.sensors.batch


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.batch.BatchSensor
   airflow.providers.amazon.aws.sensors.batch.BatchComputeEnvironmentSensor
   airflow.providers.amazon.aws.sensors.batch.BatchJobQueueSensor




.. py:class:: BatchSensor(*, job_id, aws_conn_id = 'aws_default', region_name = None, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), poke_interval = 5, max_retries = 5, **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Poll the state of the Batch Job until it reaches a terminal state; fails if the job fails.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:BatchSensor`

   :param job_id: Batch job_id to check the state for
   :param aws_conn_id: aws connection to use, defaults to 'aws_default'
   :param region_name: aws region name associated with the client
   :param deferrable: Run sensor in the deferrable mode.
   :param poke_interval: polling period in seconds to check for the status of the job.
   :param max_retries: Number of times to poll for job state before
       returning the current state.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_id',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: ui_color
      :value: '#66c3ff'



   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event)

      Execute when the trigger fires - returns immediately.

      Relies on trigger to throw an exception, otherwise it assumes execution was successful.


   .. py:method:: get_hook()

      Create and return a BatchClientHook.


   .. py:method:: hook()



.. py:class:: BatchComputeEnvironmentSensor(compute_environment, aws_conn_id = 'aws_default', region_name = None, **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Poll the state of the Batch environment until it reaches a terminal state; fails if the environment fails.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:BatchComputeEnvironmentSensor`

   :param compute_environment: Batch compute environment name

   :param aws_conn_id: aws connection to use, defaults to 'aws_default'

   :param region_name: aws region name associated with the client

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('compute_environment',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: ui_color
      :value: '#66c3ff'



   .. py:method:: hook()

      Create and return a BatchClientHook.


   .. py:method:: poke(context)

      Override when deriving this class.



.. py:class:: BatchJobQueueSensor(job_queue, treat_non_existing_as_deleted = False, aws_conn_id = 'aws_default', region_name = None, **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Poll the state of the Batch job queue until it reaches a terminal state; fails if the queue fails.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:BatchJobQueueSensor`

   :param job_queue: Batch job queue name

   :param treat_non_existing_as_deleted: If True, a non-existing Batch job queue is considered as a deleted
       queue and as such a valid case.

   :param aws_conn_id: aws connection to use, defaults to 'aws_default'

   :param region_name: aws region name associated with the client

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_queue',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: ui_color
      :value: '#66c3ff'



   .. py:method:: hook()

      Create and return a BatchClientHook.


   .. py:method:: poke(context)

      Override when deriving this class.
