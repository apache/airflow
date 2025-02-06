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

:py:mod:`airflow.providers.amazon.aws.hooks.emr`
================================================

.. py:module:: airflow.providers.amazon.aws.hooks.emr


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.emr.EmrHook
   airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook
   airflow.providers.amazon.aws.hooks.emr.EmrContainerHook




.. py:class:: EmrHook(emr_conn_id = default_conn_name, *args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Elastic MapReduce Service (EMR).

   Provide thick wrapper around :external+boto3:py:class:`boto3.client("emr") <EMR.Client>`.

   :param emr_conn_id: :ref:`Amazon Elastic MapReduce Connection <howto/connection:emr>`.
       This attribute is only necessary when using
       the :meth:`airflow.providers.amazon.aws.hooks.emr.EmrHook.create_job_flow`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:attribute:: conn_name_attr
      :value: 'emr_conn_id'



   .. py:attribute:: default_conn_name
      :value: 'emr_default'



   .. py:attribute:: conn_type
      :value: 'emr'



   .. py:attribute:: hook_name
      :value: 'Amazon Elastic MapReduce'



   .. py:method:: get_cluster_id_by_name(emr_cluster_name, cluster_states)

      Fetch id of EMR cluster with given name and (optional) states; returns only if single id is found.

      .. seealso::
          - :external+boto3:py:meth:`EMR.Client.list_clusters`

      :param emr_cluster_name: Name of a cluster to find
      :param cluster_states: State(s) of cluster to find
      :return: id of the EMR cluster


   .. py:method:: create_job_flow(job_flow_overrides)

      Create and start running a new cluster (job flow).

      .. seealso::
          - :external+boto3:py:meth:`EMR.Client.run_job_flow`

      This method uses ``EmrHook.emr_conn_id`` to receive the initial Amazon EMR cluster configuration.
      If ``EmrHook.emr_conn_id`` is empty or the connection does not exist, then an empty initial
      configuration is used.

      :param job_flow_overrides: Is used to overwrite the parameters in the initial Amazon EMR configuration
          cluster. The resulting configuration will be used in the
          :external+boto3:py:meth:`EMR.Client.run_job_flow`.

      .. seealso::
          - :ref:`Amazon Elastic MapReduce Connection <howto/connection:emr>`
          - :external+boto3:py:meth:`EMR.Client.run_job_flow`
          - `API RunJobFlow <https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html>`_


   .. py:method:: add_job_flow_steps(job_flow_id, steps = None, wait_for_completion = False, waiter_delay = None, waiter_max_attempts = None, execution_role_arn = None)

      Add new steps to a running cluster.

      .. seealso::
          - :external+boto3:py:meth:`EMR.Client.add_job_flow_steps`

      :param job_flow_id: The id of the job flow to which the steps are being added
      :param steps: A list of the steps to be executed by the job flow
      :param wait_for_completion: If True, wait for the steps to be completed. Default is False
      :param waiter_delay: The amount of time in seconds to wait between attempts. Default is 5
      :param waiter_max_attempts: The maximum number of attempts to be made. Default is 100
      :param execution_role_arn: The ARN of the runtime role for a step on the cluster.


   .. py:method:: test_connection()

      Return failed state for test Amazon Elastic MapReduce Connection (untestable).

      We need to overwrite this method because this hook is based on
      :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook`,
      otherwise it will try to test connection to AWS STS by using the default boto3 credential strategy.


   .. py:method:: get_ui_field_behaviour()
      :staticmethod:

      Return custom UI field behaviour for Amazon Elastic MapReduce Connection.



.. py:class:: EmrServerlessHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon EMR Serverless.

   Provide thin wrapper around :py:class:`boto3.client("emr-serverless") <EMRServerless.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:attribute:: JOB_INTERMEDIATE_STATES



   .. py:attribute:: JOB_FAILURE_STATES



   .. py:attribute:: JOB_SUCCESS_STATES



   .. py:attribute:: JOB_TERMINAL_STATES



   .. py:attribute:: APPLICATION_INTERMEDIATE_STATES



   .. py:attribute:: APPLICATION_FAILURE_STATES



   .. py:attribute:: APPLICATION_SUCCESS_STATES



   .. py:method:: cancel_running_jobs(application_id, waiter_config = None, wait_for_completion = True)

      Cancel jobs in an intermediate state, and return the number of cancelled jobs.

      If wait_for_completion is True, then the method will wait until all jobs are
      cancelled before returning.

      Note: if new jobs are triggered while this operation is ongoing,
      it's going to time out and return an error.



.. py:class:: EmrContainerHook(*args, virtual_cluster_id = None, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon EMR Containers (Amazon EMR on EKS).

   Provide thick wrapper around :py:class:`boto3.client("emr-containers") <EMRContainers.Client>`.

   :param virtual_cluster_id: Cluster ID of the EMR on EKS virtual cluster

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:attribute:: INTERMEDIATE_STATES
      :value: ('PENDING', 'SUBMITTED', 'RUNNING')



   .. py:attribute:: FAILURE_STATES
      :value: ('FAILED', 'CANCELLED', 'CANCEL_PENDING')



   .. py:attribute:: SUCCESS_STATES
      :value: ('COMPLETED',)



   .. py:attribute:: TERMINAL_STATES
      :value: ('COMPLETED', 'FAILED', 'CANCELLED', 'CANCEL_PENDING')



   .. py:method:: create_emr_on_eks_cluster(virtual_cluster_name, eks_cluster_name, eks_namespace, tags = None)


   .. py:method:: submit_job(name, execution_role_arn, release_label, job_driver, configuration_overrides = None, client_request_token = None, tags = None)

      Submit a job to the EMR Containers API and return the job ID.

      A job run is a unit of work, such as a Spark jar, PySpark script,
      or SparkSQL query, that you submit to Amazon EMR on EKS.

      .. seealso::
          - :external+boto3:py:meth:`EMRContainers.Client.start_job_run`

      :param name: The name of the job run.
      :param execution_role_arn: The IAM role ARN associated with the job run.
      :param release_label: The Amazon EMR release version to use for the job run.
      :param job_driver: Job configuration details, e.g. the Spark job parameters.
      :param configuration_overrides: The configuration overrides for the job run,
          specifically either application configuration or monitoring configuration.
      :param client_request_token: The client idempotency token of the job run request.
          Use this if you want to specify a unique ID to prevent two jobs from getting started.
      :param tags: The tags assigned to job runs.
      :return: The ID of the job run request.


   .. py:method:: get_job_failure_reason(job_id)

      Fetch the reason for a job failure (e.g. error message). Returns None or reason string.

      .. seealso::
          - :external+boto3:py:meth:`EMRContainers.Client.describe_job_run`

      :param job_id: The ID of the job run request.


   .. py:method:: check_query_status(job_id)

      Fetch the status of submitted job run. Returns None or one of valid query states.

      .. seealso::
          - :external+boto3:py:meth:`EMRContainers.Client.describe_job_run`

      :param job_id: The ID of the job run request.


   .. py:method:: poll_query_status(job_id, poll_interval = 30, max_polling_attempts = None)

      Poll the status of submitted job run until query state reaches final state; returns the final state.

      :param job_id: The ID of the job run request.
      :param poll_interval: Time (in seconds) to wait between calls to check query status on EMR
      :param max_polling_attempts: Number of times to poll for query state before function exits


   .. py:method:: stop_query(job_id)

      Cancel the submitted job_run.

      .. seealso::
          - :external+boto3:py:meth:`EMRContainers.Client.cancel_job_run`

      :param job_id: The ID of the job run to cancel.
