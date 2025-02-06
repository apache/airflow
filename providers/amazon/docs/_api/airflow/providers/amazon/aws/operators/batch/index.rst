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

:py:mod:`airflow.providers.amazon.aws.operators.batch`
======================================================

.. py:module:: airflow.providers.amazon.aws.operators.batch

.. autoapi-nested-parse::

   AWS Batch services.

   .. seealso::

       - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
       - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html
       - https://docs.aws.amazon.com/batch/latest/APIReference/Welcome.html



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.batch.BatchOperator
   airflow.providers.amazon.aws.operators.batch.BatchCreateComputeEnvironmentOperator




.. py:class:: BatchOperator(*, job_name, job_definition, job_queue, overrides = None, container_overrides = None, array_properties = None, node_overrides = None, share_identifier = None, scheduling_priority_override = None, parameters = None, job_id = None, waiters = None, max_retries = 4200, status_retries = None, aws_conn_id = None, region_name = None, tags = None, wait_for_completion = True, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), poll_interval = 30, awslogs_enabled = False, awslogs_fetch_interval = timedelta(seconds=30), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Execute a job on AWS Batch.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BatchOperator`

   :param job_name: the name for the job that will run on AWS Batch (templated)
   :param job_definition: the job definition name on AWS Batch
   :param job_queue: the queue name on AWS Batch
   :param overrides: DEPRECATED, use container_overrides instead with the same value.
   :param container_overrides: the `containerOverrides` parameter for boto3 (templated)
   :param node_overrides: the `nodeOverrides` parameter for boto3 (templated)
   :param share_identifier: The share identifier for the job. Don't specify this parameter if the job queue
       doesn't have a scheduling policy.
   :param scheduling_priority_override: The scheduling priority for the job.
       Jobs with a higher scheduling priority are scheduled before jobs with a lower scheduling priority.
       This overrides any scheduling priority in the job definition
   :param array_properties: the `arrayProperties` parameter for boto3
   :param parameters: the `parameters` for boto3 (templated)
   :param job_id: the job ID, usually unknown (None) until the
       submit_job operation gets the jobId defined by AWS Batch
   :param waiters: an :py:class:`.BatchWaiters` object (see note below);
       if None, polling is used with max_retries and status_retries.
   :param max_retries: exponential back-off retries, 4200 = 48 hours;
       polling is only used when waiters is None
   :param status_retries: number of HTTP retries to get job status, 10;
       polling is only used when waiters is None
   :param aws_conn_id: connection id of AWS credentials / region name. If None,
       credential boto3 strategy will be used.
   :param region_name: region name to use in AWS Hook.
       Override the region_name in connection (if provided)
   :param tags: collection of tags to apply to the AWS Batch job submission
       if None, no tags are submitted
   :param deferrable: Run operator in the deferrable mode.
   :param awslogs_enabled: Specifies whether logs from CloudWatch
       should be printed or not, False.
       If it is an array job, only the logs of the first task will be printed.
   :param awslogs_fetch_interval: The interval with which cloudwatch logs are to be fetched, 30 sec.
   :param poll_interval: (Deferrable mode only) Time in seconds to wait between polling.

   .. note::
       Any custom waiters must return a waiter for these calls:
       .. code-block:: python

           waiter = waiters.get_waiter("JobExists")
           waiter = waiters.get_waiter("JobRunning")
           waiter = waiters.get_waiter("JobComplete")

   .. py:property:: operator_extra_links


   .. py:attribute:: ui_color
      :value: '#c3dae0'



   .. py:attribute:: arn
      :type: str | None



   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_id', 'job_name', 'job_definition', 'job_queue', 'container_overrides', 'array_properties',...



   .. py:attribute:: template_fields_renderers



   .. py:method:: hook()


   .. py:method:: execute(context)

      Submit and monitor an AWS Batch job.

      :raises: AirflowException


   .. py:method:: execute_complete(context, event=None)


   .. py:method:: on_kill()

      Override this method to clean up subprocesses when a task instance gets killed.

      Any use of the threading, subprocess or multiprocessing module within an
      operator needs to be cleaned up, or it will leave ghost processes behind.


   .. py:method:: submit_job(context)

      Submit an AWS Batch job.

      :raises: AirflowException


   .. py:method:: monitor_job(context)

      Monitor an AWS Batch job.

      This can raise an exception or an AirflowTaskTimeout if the task was
      created with ``execution_timeout``.



.. py:class:: BatchCreateComputeEnvironmentOperator(compute_environment_name, environment_type, state, compute_resources, unmanaged_v_cpus = None, service_role = None, tags = None, poll_interval = 30, max_retries = None, aws_conn_id = None, region_name = None, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Create an AWS Batch compute environment.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BatchCreateComputeEnvironmentOperator`

   :param compute_environment_name: Name of the AWS batch compute
       environment (templated).
   :param environment_type: Type of the compute-environment.
   :param state: State of the compute-environment.
   :param compute_resources: Details about the resources managed by the
       compute-environment (templated). More details:
       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html#Batch.Client.create_compute_environment
   :param unmanaged_v_cpus: Maximum number of vCPU for an unmanaged compute
       environment. This parameter is only supported when the ``type``
       parameter is set to ``UNMANAGED``.
   :param service_role: IAM role that allows Batch to make calls to other AWS
       services on your behalf (templated).
   :param tags: Tags that you apply to the compute-environment to help you
       categorize and organize your resources.
   :param poll_interval: How long to wait in seconds between 2 polls at the environment status.
       Only useful when deferrable is True.
   :param max_retries: How many times to poll for the environment status.
       Only useful when deferrable is True.
   :param aws_conn_id: Connection ID of AWS credentials / region name. If None,
       credential boto3 strategy will be used.
   :param region_name: Region name to use in AWS Hook. Overrides the
       ``region_name`` in connection if provided.
   :param deferrable: If True, the operator will wait asynchronously for the environment to be created.
       This mode requires aiobotocore module to be installed. (default: False)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('compute_environment_name', 'compute_resources', 'service_role')



   .. py:attribute:: template_fields_renderers



   .. py:method:: hook()

      Create and return a BatchClientHook.


   .. py:method:: execute(context)

      Create an AWS batch compute environment.


   .. py:method:: execute_complete(context, event=None)
