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

:py:mod:`airflow.providers.amazon.aws.triggers.batch`
=====================================================

.. py:module:: airflow.providers.amazon.aws.triggers.batch


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.triggers.batch.BatchOperatorTrigger
   airflow.providers.amazon.aws.triggers.batch.BatchSensorTrigger
   airflow.providers.amazon.aws.triggers.batch.BatchJobTrigger
   airflow.providers.amazon.aws.triggers.batch.BatchCreateComputeEnvironmentTrigger




.. py:class:: BatchOperatorTrigger(job_id = None, max_retries = 10, aws_conn_id = 'aws_default', region_name = None, poll_interval = 30)


   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Asynchronously poll the boto3 API and wait for the Batch job to be in the `SUCCEEDED` state.

   :param job_id:  A unique identifier for the cluster.
   :param max_retries: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param region_name: region name to use in AWS Hook
   :param poll_interval: The amount of time in seconds to wait between attempts.

   .. py:method:: serialize()

      Serialize BatchOperatorTrigger arguments and classpath.


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



.. py:class:: BatchSensorTrigger(job_id, region_name, aws_conn_id = 'aws_default', poke_interval = 5)


   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Checks for the status of a submitted job_id to AWS Batch until it reaches a failure or a success state.

   BatchSensorTrigger is fired as deferred class with params to poll the job state in Triggerer.

   :param job_id: the job ID, to poll for job completion or not
   :param region_name: AWS region name to use
       Override the region_name in connection (if provided)
   :param aws_conn_id: connection id of AWS credentials / region name. If None,
       credential boto3 strategy will be used
   :param poke_interval: polling period in seconds to check for the status of the job

   .. py:method:: serialize()

      Serialize BatchSensorTrigger arguments and classpath.


   .. py:method:: hook()


   .. py:method:: run()
      :async:

      Make async connection using aiobotocore library to AWS Batch, periodically poll for the job status.

      The status that indicates job completion are: 'SUCCEEDED'|'FAILED'.



.. py:class:: BatchJobTrigger(job_id, region_name = None, aws_conn_id = 'aws_default', waiter_delay = 5, waiter_max_attempts = 720)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Checks for the status of a submitted job_id to AWS Batch until it reaches a failure or a success state.

   :param job_id: the job ID, to poll for job completion or not
   :param region_name: AWS region name to use
       Override the region_name in connection (if provided)
   :param aws_conn_id: connection id of AWS credentials / region name. If None,
       credential boto3 strategy will be used
   :param waiter_delay: polling period in seconds to check for the status of the job
   :param waiter_max_attempts: The maximum number of attempts to be made.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: BatchCreateComputeEnvironmentTrigger(compute_env_arn, waiter_delay = 30, waiter_max_attempts = 10, aws_conn_id = 'aws_default', region_name = None)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Asynchronously poll the boto3 API and wait for the compute environment to be ready.

   :param compute_env_arn: The ARN of the compute env.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param region_name: region name to use in AWS Hook
   :param waiter_delay: The amount of time in seconds to wait between attempts.

   .. py:method:: hook()

      Override in subclasses to return the right hook.
