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

:py:mod:`airflow.providers.amazon.aws.triggers.sagemaker`
=========================================================

.. py:module:: airflow.providers.amazon.aws.triggers.sagemaker


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.triggers.sagemaker.SageMakerTrigger
   airflow.providers.amazon.aws.triggers.sagemaker.SageMakerPipelineTrigger




.. py:class:: SageMakerTrigger(job_name, job_type, poke_interval = 30, max_attempts = 480, aws_conn_id = 'aws_default')


   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   SageMakerTrigger is fired as deferred class with params to run the task in triggerer.

   :param job_name: name of the job to check status
   :param job_type: Type of the sagemaker job whether it is Transform or Training
   :param poke_interval:  polling period in seconds to check for the status
   :param max_attempts: Number of times to poll for query state before returning the current state,
       defaults to None.
   :param aws_conn_id: AWS connection ID for sagemaker

   .. py:method:: serialize()

      Serialize SagemakerTrigger arguments and classpath.


   .. py:method:: hook()


   .. py:method:: run()
      :async:

      Run the trigger in an asynchronous context.

      The trigger should yield an Event whenever it wants to fire off
      an event, and return None if it is finished. Single-event triggers
      should thus yield and then immediately return.

      If it yields, it is likely that it will be resumed very quickly,
      but it may not be (e.g. if the workload is being moved to another
      triggerer process, or a multi-event trigger was being used for a
      single-event task defer).

      In either case, Trigger classes should assume they will be persisted,
      and then rely on cleanup() being called when they are no longer needed.



.. py:class:: SageMakerPipelineTrigger(waiter_type, pipeline_execution_arn, waiter_delay, waiter_max_attempts, aws_conn_id)


   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Trigger to wait for a sagemaker pipeline execution to finish.

   .. py:class:: Type


      Bases: :py:obj:`enum.IntEnum`

      Type of waiter to use.

      .. py:attribute:: COMPLETE
         :value: 1



      .. py:attribute:: STOPPED
         :value: 2




   .. py:method:: serialize()

      Return the information needed to reconstruct this Trigger.

      :return: Tuple of (class path, keyword arguments needed to re-instantiate).


   .. py:method:: run()
      :async:

      Run the trigger in an asynchronous context.

      The trigger should yield an Event whenever it wants to fire off
      an event, and return None if it is finished. Single-event triggers
      should thus yield and then immediately return.

      If it yields, it is likely that it will be resumed very quickly,
      but it may not be (e.g. if the workload is being moved to another
      triggerer process, or a multi-event trigger was being used for a
      single-event task defer).

      In either case, Trigger classes should assume they will be persisted,
      and then rely on cleanup() being called when they are no longer needed.
