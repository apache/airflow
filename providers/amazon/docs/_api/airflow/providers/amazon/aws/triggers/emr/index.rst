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

:py:mod:`airflow.providers.amazon.aws.triggers.emr`
===================================================

.. py:module:: airflow.providers.amazon.aws.triggers.emr


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.triggers.emr.EmrAddStepsTrigger
   airflow.providers.amazon.aws.triggers.emr.EmrCreateJobFlowTrigger
   airflow.providers.amazon.aws.triggers.emr.EmrTerminateJobFlowTrigger
   airflow.providers.amazon.aws.triggers.emr.EmrContainerTrigger
   airflow.providers.amazon.aws.triggers.emr.EmrStepSensorTrigger
   airflow.providers.amazon.aws.triggers.emr.EmrServerlessCreateApplicationTrigger
   airflow.providers.amazon.aws.triggers.emr.EmrServerlessStartApplicationTrigger
   airflow.providers.amazon.aws.triggers.emr.EmrServerlessStopApplicationTrigger
   airflow.providers.amazon.aws.triggers.emr.EmrServerlessStartJobTrigger
   airflow.providers.amazon.aws.triggers.emr.EmrServerlessDeleteApplicationTrigger
   airflow.providers.amazon.aws.triggers.emr.EmrServerlessCancelJobsTrigger




.. py:class:: EmrAddStepsTrigger(job_flow_id, step_ids, waiter_delay, waiter_max_attempts, aws_conn_id = 'aws_default')


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Poll for the status of EMR steps until they reach terminal state.

   :param job_flow_id: job_flow_id which contains the steps to check the state of
   :param step_ids: steps to check the state of
   :param waiter_delay: polling period in seconds to check for the status
   :param waiter_max_attempts: The maximum number of attempts to be made
   :param aws_conn_id: Reference to AWS connection id


   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: EmrCreateJobFlowTrigger(job_flow_id, poll_interval = None, max_attempts = None, aws_conn_id = None, waiter_delay = 30, waiter_max_attempts = 60)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Asynchronously poll the boto3 API and wait for the JobFlow to finish executing.

   :param job_flow_id: The id of the job flow to wait for.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: EmrTerminateJobFlowTrigger(job_flow_id, poll_interval = None, max_attempts = None, aws_conn_id = None, waiter_delay = 30, waiter_max_attempts = 60)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Asynchronously poll the boto3 API and wait for the JobFlow to finish terminating.

   :param job_flow_id: ID of the EMR Job Flow to terminate
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: EmrContainerTrigger(virtual_cluster_id, job_id, aws_conn_id = 'aws_default', poll_interval = None, waiter_delay = 30, waiter_max_attempts = 600)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Poll for the status of EMR container until reaches terminal state.

   :param virtual_cluster_id: Reference Emr cluster id
   :param job_id:  job_id to check the state
   :param aws_conn_id: Reference to AWS connection id
   :param waiter_delay: polling period in seconds to check for the status

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: EmrStepSensorTrigger(job_flow_id, step_id, waiter_delay = 30, waiter_max_attempts = 60, aws_conn_id = 'aws_default')


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Poll for the status of EMR container until reaches terminal state.

   :param job_flow_id: job_flow_id which contains the step check the state of
   :param step_id:  step to check the state of
   :param waiter_delay: polling period in seconds to check for the status
   :param waiter_max_attempts: The maximum number of attempts to be made
   :param aws_conn_id: Reference to AWS connection id

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: EmrServerlessCreateApplicationTrigger(application_id, waiter_delay = 30, waiter_max_attempts = 60, aws_conn_id = 'aws_default')


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Poll an Emr Serverless application and wait for it to be created.

   :param application_id: The ID of the application being polled.
   :waiter_delay: polling period in seconds to check for the status
   :param waiter_max_attempts: The maximum number of attempts to be made
   :param aws_conn_id: Reference to AWS connection id

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: EmrServerlessStartApplicationTrigger(application_id, waiter_delay = 30, waiter_max_attempts = 60, aws_conn_id = 'aws_default')


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Poll an Emr Serverless application and wait for it to be started.

   :param application_id: The ID of the application being polled.
   :waiter_delay: polling period in seconds to check for the status
   :param waiter_max_attempts: The maximum number of attempts to be made
   :param aws_conn_id: Reference to AWS connection id

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: EmrServerlessStopApplicationTrigger(application_id, waiter_delay = 30, waiter_max_attempts = 60, aws_conn_id = 'aws_default')


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Poll an Emr Serverless application and wait for it to be stopped.

   :param application_id: The ID of the application being polled.
   :waiter_delay: polling period in seconds to check for the status
   :param waiter_max_attempts: The maximum number of attempts to be made
   :param aws_conn_id: Reference to AWS connection id.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: EmrServerlessStartJobTrigger(application_id, job_id, waiter_delay = 30, waiter_max_attempts = 60, aws_conn_id = 'aws_default')


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Poll an Emr Serverless job run and wait for it to be completed.

   :param application_id: The ID of the application the job in being run on.
   :param job_id: The ID of the job run.
   :waiter_delay: polling period in seconds to check for the status
   :param waiter_max_attempts: The maximum number of attempts to be made
   :param aws_conn_id: Reference to AWS connection id

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: EmrServerlessDeleteApplicationTrigger(application_id, waiter_delay = 30, waiter_max_attempts = 60, aws_conn_id = 'aws_default')


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Poll an Emr Serverless application and wait for it to be deleted.

   :param application_id: The ID of the application being polled.
   :waiter_delay: polling period in seconds to check for the status
   :param waiter_max_attempts: The maximum number of attempts to be made
   :param aws_conn_id: Reference to AWS connection id

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: EmrServerlessCancelJobsTrigger(application_id, aws_conn_id, waiter_delay, waiter_max_attempts)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Trigger for canceling a list of jobs in an EMR Serverless application.

   :param application_id: EMR Serverless application ID
   :param aws_conn_id: Reference to AWS connection id
   :param waiter_delay: Delay in seconds between each attempt to check the status
   :param waiter_max_attempts: Maximum number of attempts to check the status

   .. py:method:: hook()

      Override in subclasses to return the right hook.
