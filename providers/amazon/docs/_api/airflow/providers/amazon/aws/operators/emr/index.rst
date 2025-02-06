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

:py:mod:`airflow.providers.amazon.aws.operators.emr`
====================================================

.. py:module:: airflow.providers.amazon.aws.operators.emr


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.emr.EmrAddStepsOperator
   airflow.providers.amazon.aws.operators.emr.EmrStartNotebookExecutionOperator
   airflow.providers.amazon.aws.operators.emr.EmrStopNotebookExecutionOperator
   airflow.providers.amazon.aws.operators.emr.EmrEksCreateClusterOperator
   airflow.providers.amazon.aws.operators.emr.EmrContainerOperator
   airflow.providers.amazon.aws.operators.emr.EmrCreateJobFlowOperator
   airflow.providers.amazon.aws.operators.emr.EmrModifyClusterOperator
   airflow.providers.amazon.aws.operators.emr.EmrTerminateJobFlowOperator
   airflow.providers.amazon.aws.operators.emr.EmrServerlessCreateApplicationOperator
   airflow.providers.amazon.aws.operators.emr.EmrServerlessStartJobOperator
   airflow.providers.amazon.aws.operators.emr.EmrServerlessStopApplicationOperator
   airflow.providers.amazon.aws.operators.emr.EmrServerlessDeleteApplicationOperator




.. py:class:: EmrAddStepsOperator(*, job_flow_id = None, job_flow_name = None, cluster_states = None, aws_conn_id = 'aws_default', steps = None, wait_for_completion = False, waiter_delay = 30, waiter_max_attempts = 60, execution_role_arn = None, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   An operator that adds steps to an existing EMR job_flow.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EmrAddStepsOperator`

   :param job_flow_id: id of the JobFlow to add steps to. (templated)
   :param job_flow_name: name of the JobFlow to add steps to. Use as an alternative to passing
       job_flow_id. will search for id of JobFlow with matching name in one of the states in
       param cluster_states. Exactly one cluster like this should exist or will fail. (templated)
   :param cluster_states: Acceptable cluster states when searching for JobFlow id by job_flow_name.
       (templated)
   :param aws_conn_id: aws connection to uses
   :param steps: boto3 style steps or reference to a steps file (must be '.json') to
       be added to the jobflow. (templated)
   :param wait_for_completion: If True, the operator will wait for all the steps to be completed.
   :param execution_role_arn: The ARN of the runtime role for a step on the cluster.
   :param do_xcom_push: if True, job_flow_id is pushed to XCom with key job_flow_id.
   :param wait_for_completion: Whether to wait for job run completion. (default: True)
   :param deferrable: If True, the operator will wait asynchronously for the job to complete.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_flow_id', 'job_flow_name', 'cluster_states', 'steps', 'execution_role_arn')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ('.json',)



   .. py:attribute:: template_fields_renderers



   .. py:attribute:: ui_color
      :value: '#f9c915'



   .. py:attribute:: operator_extra_links
      :value: ()



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: EmrStartNotebookExecutionOperator(editor_id, relative_path, cluster_id, service_role, notebook_execution_name = None, notebook_params = None, notebook_instance_security_group_id = None, master_instance_security_group_id = None, tags = None, wait_for_completion = False, aws_conn_id = 'aws_default', waiter_max_attempts = NOTSET, waiter_delay = NOTSET, waiter_countdown = 25 * 60, waiter_check_interval_seconds = 60, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   An operator that starts an EMR notebook execution.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EmrStartNotebookExecutionOperator`

   :param editor_id: The unique identifier of the EMR notebook to use for notebook execution.
   :param relative_path: The path and file name of the notebook file for this execution,
       relative to the path specified for the EMR notebook.
   :param cluster_id: The unique identifier of the EMR cluster the notebook is attached to.
   :param service_role: The name or ARN of the IAM role that is used as the service role
       for Amazon EMR (the EMR role) for the notebook execution.
   :param notebook_execution_name: Optional name for the notebook execution.
   :param notebook_params: Input parameters in JSON format passed to the EMR notebook at
       runtime for execution.
   :param: notebook_instance_security_group_id: The unique identifier of the Amazon EC2
       security group to associate with the EMR notebook for this notebook execution.
   :param: master_instance_security_group_id: Optional unique ID of an EC2 security
       group to associate with the instance of the EMR cluster for this notebook execution.
   :param tags: Optional list of key value pair to associate with the notebook execution.
   :param waiter_max_attempts: Maximum number of tries before failing.
   :param waiter_delay: Number of seconds between polling the state of the notebook.

   :param waiter_countdown: Total amount of time the operator will wait for the notebook to stop.
       Defaults to 25 * 60 seconds. (Deprecated.  Please use waiter_max_attempts.)
   :param waiter_check_interval_seconds: Number of seconds between polling the state of the notebook.
       Defaults to 60 seconds. (Deprecated.  Please use waiter_delay.)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('editor_id', 'cluster_id', 'relative_path', 'service_role', 'notebook_execution_name',...



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: EmrStopNotebookExecutionOperator(notebook_execution_id, wait_for_completion = False, aws_conn_id = 'aws_default', waiter_max_attempts = NOTSET, waiter_delay = NOTSET, waiter_countdown = 25 * 60, waiter_check_interval_seconds = 60, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   An operator that stops a running EMR notebook execution.

    .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EmrStopNotebookExecutionOperator`

   :param notebook_execution_id: The unique identifier of the notebook execution.
   :param wait_for_completion: If True, the operator will wait for the notebook.
       to be in a STOPPED or FINISHED state. Defaults to False.
   :param aws_conn_id: aws connection to use.
   :param waiter_max_attempts: Maximum number of tries before failing.
   :param waiter_delay: Number of seconds between polling the state of the notebook.

   :param waiter_countdown: Total amount of time the operator will wait for the notebook to stop.
       Defaults to 25 * 60 seconds. (Deprecated.  Please use waiter_max_attempts.)
   :param waiter_check_interval_seconds: Number of seconds between polling the state of the notebook.
       Defaults to 60 seconds. (Deprecated.  Please use waiter_delay.)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('notebook_execution_id', 'waiter_delay', 'waiter_max_attempts')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: EmrEksCreateClusterOperator(*, virtual_cluster_name, eks_cluster_name, eks_namespace, virtual_cluster_id = '', aws_conn_id = 'aws_default', tags = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   An operator that creates EMR on EKS virtual clusters.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EmrEksCreateClusterOperator`

   :param virtual_cluster_name: The name of the EMR EKS virtual cluster to create.
   :param eks_cluster_name: The EKS cluster used by the EMR virtual cluster.
   :param eks_namespace: namespace used by the EKS cluster.
   :param virtual_cluster_id: The EMR on EKS virtual cluster id.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param tags: The tags assigned to created cluster.
       Defaults to None

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('virtual_cluster_name', 'eks_cluster_name', 'eks_namespace')



   .. py:attribute:: ui_color
      :value: '#f9c915'



   .. py:method:: hook()

      Create and return an EmrContainerHook.


   .. py:method:: execute(context)

      Create EMR on EKS virtual Cluster.



.. py:class:: EmrContainerOperator(*, name, virtual_cluster_id, execution_role_arn, release_label, job_driver, configuration_overrides = None, client_request_token = None, aws_conn_id = 'aws_default', wait_for_completion = True, poll_interval = 30, max_tries = None, tags = None, max_polling_attempts = None, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   An operator that submits jobs to EMR on EKS virtual clusters.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EmrContainerOperator`

   :param name: The name of the job run.
   :param virtual_cluster_id: The EMR on EKS virtual cluster ID
   :param execution_role_arn: The IAM role ARN associated with the job run.
   :param release_label: The Amazon EMR release version to use for the job run.
   :param job_driver: Job configuration details, e.g. the Spark job parameters.
   :param configuration_overrides: The configuration overrides for the job run,
       specifically either application configuration or monitoring configuration.
   :param client_request_token: The client idempotency token of the job run request.
       Use this if you want to specify a unique ID to prevent two jobs from getting started.
       If no token is provided, a UUIDv4 token will be generated for you.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param wait_for_completion: Whether or not to wait in the operator for the job to complete.
   :param poll_interval: Time (in seconds) to wait between two consecutive calls to check query status on EMR
   :param max_tries: Deprecated - use max_polling_attempts instead.
   :param max_polling_attempts: Maximum number of times to wait for the job run to finish.
       Defaults to None, which will poll until the job is *not* in a pending, submitted, or running state.
   :param tags: The tags assigned to job runs.
       Defaults to None
   :param deferrable: Run operator in the deferrable mode.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('name', 'virtual_cluster_id', 'execution_role_arn', 'release_label', 'job_driver',...



   .. py:attribute:: ui_color
      :value: '#f9c915'



   .. py:method:: hook()

      Create and return an EmrContainerHook.


   .. py:method:: execute(context)

      Run job on EMR Containers.


   .. py:method:: check_failure(query_status)


   .. py:method:: execute_complete(context, event=None)


   .. py:method:: on_kill()

      Cancel the submitted job run.



.. py:class:: EmrCreateJobFlowOperator(*, aws_conn_id = 'aws_default', emr_conn_id = 'emr_default', job_flow_overrides = None, region_name = None, wait_for_completion = False, waiter_max_attempts = NOTSET, waiter_delay = NOTSET, waiter_countdown = None, waiter_check_interval_seconds = 60, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Creates an EMR JobFlow, reading the config from the EMR connection.

   A dictionary of JobFlow overrides can be passed that override the config from the connection.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EmrCreateJobFlowOperator`

   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node)
   :param emr_conn_id: :ref:`Amazon Elastic MapReduce Connection <howto/connection:emr>`.
       Use to receive an initial Amazon EMR cluster configuration:
       ``boto3.client('emr').run_job_flow`` request body.
       If this is None or empty or the connection does not exist,
       then an empty initial configuration is used.
   :param job_flow_overrides: boto3 style arguments or reference to an arguments file
       (must be '.json') to override specific ``emr_conn_id`` extra parameters. (templated)
   :param region_name: Region named passed to EmrHook
   :param wait_for_completion: Whether to finish task immediately after creation (False) or wait for jobflow
       completion (True)
   :param waiter_max_attempts: Maximum number of tries before failing.
   :param waiter_delay: Number of seconds between polling the state of the notebook.

   :param waiter_countdown: Max. seconds to wait for jobflow completion (only in combination with
       wait_for_completion=True, None = no limit) (Deprecated.  Please use waiter_max_attempts.)
   :param waiter_check_interval_seconds: Number of seconds between polling the jobflow state. Defaults to 60
       seconds. (Deprecated.  Please use waiter_delay.)
   :param deferrable: If True, the operator will wait asynchronously for the crawl to complete.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_flow_overrides', 'waiter_delay', 'waiter_max_attempts')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ('.json',)



   .. py:attribute:: template_fields_renderers



   .. py:attribute:: ui_color
      :value: '#f9c915'



   .. py:attribute:: operator_extra_links
      :value: ()



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)


   .. py:method:: on_kill()

      Terminate the EMR cluster (job flow) unless TerminationProtected is enabled on the cluster.



.. py:class:: EmrModifyClusterOperator(*, cluster_id, step_concurrency_level, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   An operator that modifies an existing EMR cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EmrModifyClusterOperator`

   :param cluster_id: cluster identifier
   :param step_concurrency_level: Concurrency of the cluster
   :param aws_conn_id: aws connection to uses
   :param do_xcom_push: if True, cluster_id is pushed to XCom with key cluster_id.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_id', 'step_concurrency_level')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: ui_color
      :value: '#f9c915'



   .. py:attribute:: operator_extra_links
      :value: ()



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: EmrTerminateJobFlowOperator(*, job_flow_id, aws_conn_id = 'aws_default', waiter_delay = 60, waiter_max_attempts = 20, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Operator to terminate EMR JobFlows.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EmrTerminateJobFlowOperator`

   :param job_flow_id: id of the JobFlow to terminate. (templated)
   :param aws_conn_id: aws connection to uses
   :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check JobFlow status
   :param waiter_max_attempts: The maximum number of times to poll for JobFlow status.
   :param deferrable: If True, the operator will wait asynchronously for the crawl to complete.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_flow_id',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: ui_color
      :value: '#f9c915'



   .. py:attribute:: operator_extra_links
      :value: ()



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: EmrServerlessCreateApplicationOperator(release_label, job_type, client_request_token = '', config = None, wait_for_completion = True, aws_conn_id = 'aws_default', waiter_countdown = NOTSET, waiter_check_interval_seconds = NOTSET, waiter_max_attempts = NOTSET, waiter_delay = NOTSET, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Operator to create Serverless EMR Application.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EmrServerlessCreateApplicationOperator`

   :param release_label: The EMR release version associated with the application.
   :param job_type: The type of application you want to start, such as Spark or Hive.
   :param wait_for_completion: If true, wait for the Application to start before returning. Default to True.
       If set to False, ``waiter_max_attempts`` and ``waiter_delay`` will only be applied when
       waiting for the application to be in the ``CREATED`` state.
   :param client_request_token: The client idempotency token of the application to create.
     Its value must be unique for each request.
   :param config: Optional dictionary for arbitrary parameters to the boto API create_application call.
   :param aws_conn_id: AWS connection to use
   :param waiter_countdown: (deprecated) Total amount of time, in seconds, the operator will wait for
       the application to start. Defaults to 25 minutes.
   :param waiter_check_interval_seconds: (deprecated) Number of seconds between polling the state
       of the application. Defaults to 60 seconds.
   :waiter_max_attempts: Number of times the waiter should poll the application to check the state.
       If not set, the waiter will use its default value.
   :param waiter_delay: Number of seconds between polling the state of the application.
   :param deferrable: If True, the operator will wait asynchronously for application to be created.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False, but can be overridden in config file by setting default_deferrable to True)

   .. py:method:: hook()

      Create and return an EmrServerlessHook.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: start_application_deferred(context, event = None)


   .. py:method:: execute_complete(context, event = None)



.. py:class:: EmrServerlessStartJobOperator(application_id, execution_role_arn, job_driver, configuration_overrides, client_request_token = '', config = None, wait_for_completion = True, aws_conn_id = 'aws_default', name = None, waiter_countdown = NOTSET, waiter_check_interval_seconds = NOTSET, waiter_max_attempts = NOTSET, waiter_delay = NOTSET, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Operator to start EMR Serverless job.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EmrServerlessStartJobOperator`

   :param application_id: ID of the EMR Serverless application to start.
   :param execution_role_arn: ARN of role to perform action.
   :param job_driver: Driver that the job runs on.
   :param configuration_overrides: Configuration specifications to override existing configurations.
   :param client_request_token: The client idempotency token of the application to create.
     Its value must be unique for each request.
   :param config: Optional dictionary for arbitrary parameters to the boto API start_job_run call.
   :param wait_for_completion: If true, waits for the job to start before returning. Defaults to True.
       If set to False, ``waiter_countdown`` and ``waiter_check_interval_seconds`` will only be applied
       when waiting for the application be to in the ``STARTED`` state.
   :param aws_conn_id: AWS connection to use.
   :param name: Name for the EMR Serverless job. If not provided, a default name will be assigned.
   :param waiter_countdown: (deprecated) Total amount of time, in seconds, the operator will wait for
       the job finish. Defaults to 25 minutes.
   :param waiter_check_interval_seconds: (deprecated) Number of seconds between polling the state of the job.
       Defaults to 60 seconds.
   :waiter_max_attempts: Number of times the waiter should poll the application to check the state.
       If not set, the waiter will use its default value.
   :param waiter_delay: Number of seconds between polling the state of the job run.
   :param deferrable: If True, the operator will wait asynchronously for the crawl to complete.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False, but can be overridden in config file by setting default_deferrable to True)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('application_id', 'config', 'execution_role_arn', 'job_driver', 'configuration_overrides')



   .. py:attribute:: template_fields_renderers



   .. py:method:: hook()

      Create and return an EmrServerlessHook.


   .. py:method:: execute(context, event = None)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event = None)


   .. py:method:: on_kill()

      Cancel the submitted job run.

      Note: this method will not run in deferrable mode.



.. py:class:: EmrServerlessStopApplicationOperator(application_id, wait_for_completion = True, aws_conn_id = 'aws_default', waiter_countdown = NOTSET, waiter_check_interval_seconds = NOTSET, waiter_max_attempts = NOTSET, waiter_delay = NOTSET, force_stop = False, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Operator to stop an EMR Serverless application.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EmrServerlessStopApplicationOperator`

   :param application_id: ID of the EMR Serverless application to stop.
   :param wait_for_completion: If true, wait for the Application to stop before returning. Default to True
   :param aws_conn_id: AWS connection to use
   :param waiter_countdown: (deprecated) Total amount of time, in seconds, the operator will wait for
       the application be stopped. Defaults to 5 minutes.
   :param waiter_check_interval_seconds: (deprecated) Number of seconds between polling the state of the
       application. Defaults to 60 seconds.
   :param force_stop: If set to True, any job for that app that is not in a terminal state will be cancelled.
       Otherwise, trying to stop an app with running jobs will return an error.
       If you want to wait for the jobs to finish gracefully, use
       :class:`airflow.providers.amazon.aws.sensors.emr.EmrServerlessJobSensor`
   :waiter_max_attempts: Number of times the waiter should poll the application to check the state.
       Default is 25.
   :param waiter_delay: Number of seconds between polling the state of the application.
       Default is 60 seconds.
   :param deferrable: If True, the operator will wait asynchronously for the application to stop.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False, but can be overridden in config file by setting default_deferrable to True)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('application_id',)



   .. py:method:: hook()

      Create and return an EmrServerlessHook.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: stop_application(context, event = None)


   .. py:method:: execute_complete(context, event = None)



.. py:class:: EmrServerlessDeleteApplicationOperator(application_id, wait_for_completion = True, aws_conn_id = 'aws_default', waiter_countdown = NOTSET, waiter_check_interval_seconds = NOTSET, waiter_max_attempts = NOTSET, waiter_delay = NOTSET, force_stop = False, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`EmrServerlessStopApplicationOperator`

   Operator to delete EMR Serverless application.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EmrServerlessDeleteApplicationOperator`

   :param application_id: ID of the EMR Serverless application to delete.
   :param wait_for_completion: If true, wait for the Application to be deleted before returning.
       Defaults to True. Note that this operator will always wait for the application to be STOPPED first.
   :param aws_conn_id: AWS connection to use
   :param waiter_countdown: (deprecated) Total amount of time, in seconds, the operator will wait for each
       step of first,the application to be stopped, and then deleted. Defaults to 25 minutes.
   :param waiter_check_interval_seconds: (deprecated) Number of seconds between polling the state
       of the application. Defaults to 60 seconds.
   :waiter_max_attempts: Number of times the waiter should poll the application to check the state.
       Defaults to 25.
   :param waiter_delay: Number of seconds between polling the state of the application.
       Defaults to 60 seconds.
   :param deferrable: If True, the operator will wait asynchronously for application to be deleted.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False, but can be overridden in config file by setting default_deferrable to True)
   :param force_stop: If set to True, any job for that app that is not in a terminal state will be cancelled.
       Otherwise, trying to delete an app with running jobs will return an error.
       If you want to wait for the jobs to finish gracefully, use
       :class:`airflow.providers.amazon.aws.sensors.emr.EmrServerlessJobSensor`

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('application_id',)



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event = None)
