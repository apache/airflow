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

:py:mod:`airflow.providers.amazon.aws.operators.sagemaker`
==========================================================

.. py:module:: airflow.providers.amazon.aws.operators.sagemaker


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.sagemaker.SageMakerBaseOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerProcessingOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerEndpointConfigOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerEndpointOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerTuningOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerModelOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerTrainingOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerDeleteModelOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerStartPipelineOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerStopPipelineOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerRegisterModelVersionOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerAutoMLOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerCreateExperimentOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerCreateNotebookOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerStopNotebookOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerDeleteNotebookOperator
   airflow.providers.amazon.aws.operators.sagemaker.SageMakerStartNoteBookOperator



Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.sagemaker.serialize



Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.sagemaker.DEFAULT_CONN_ID
   airflow.providers.amazon.aws.operators.sagemaker.CHECK_INTERVAL_SECOND


.. py:data:: DEFAULT_CONN_ID
   :type: str
   :value: 'aws_default'



.. py:data:: CHECK_INTERVAL_SECOND
   :type: int
   :value: 30



.. py:function:: serialize(result)


.. py:class:: SageMakerBaseOperator(*, config, aws_conn_id = DEFAULT_CONN_ID, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   This is the base operator for all SageMaker operators.

   :param config: The configuration necessary to start a training job (templated)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('config',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: template_fields_renderers
      :type: dict



   .. py:attribute:: ui_color
      :type: str
      :value: '#ededed'



   .. py:attribute:: integer_fields
      :type: list[list[Any]]
      :value: []



   .. py:method:: parse_integer(config, field)

      Recursive method for parsing string fields holding integer values to integers.


   .. py:method:: parse_config_integers()

      Parse the integer fields to ints in case the config is rendered by Jinja and all fields are str.


   .. py:method:: expand_role()

      Call boto3's `expand_role`, which expands an IAM role name into an ARN.


   .. py:method:: preprocess_config()

      Process the config into a usable form.


   .. py:method:: execute(context)
      :abstractmethod:

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: hook()

      Return SageMakerHook.


   .. py:method:: path_to_s3_dataset(path)
      :staticmethod:



.. py:class:: SageMakerProcessingOperator(*, config, aws_conn_id = DEFAULT_CONN_ID, wait_for_completion = True, print_log = True, check_interval = CHECK_INTERVAL_SECOND, max_attempts = None, max_ingestion_time = None, action_if_job_exists = 'timestamp', deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`SageMakerBaseOperator`

   Use Amazon SageMaker Processing to analyze data and evaluate machine learning models on Amazon SageMaker.

   With Processing, you can use a simplified, managed experience on SageMaker
   to run your data processing workloads, such as feature engineering, data
   validation, model evaluation, and model interpretation.

    .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerProcessingOperator`

   :param config: The configuration necessary to start a processing job (templated).
       For details of the configuration parameter see :py:meth:`SageMaker.Client.create_processing_job`
   :param aws_conn_id: The AWS connection ID to use.
   :param wait_for_completion: If wait is set to True, the time interval, in seconds,
       that the operation waits to check the status of the processing job.
   :param print_log: if the operator should print the cloudwatch log during processing
   :param check_interval: if wait is set to be true, this is the time interval
       in seconds which the operator will check the status of the processing job
   :param max_attempts: Number of times to poll for query state before returning the current state,
       defaults to None.
   :param max_ingestion_time: If wait is set to True, the operation fails if the processing job
       doesn't finish within max_ingestion_time seconds. If you set this parameter to None,
       the operation does not timeout.
   :param action_if_job_exists: Behaviour if the job name already exists. Possible options are "timestamp"
       (default), "increment" (deprecated) and "fail".
   :param deferrable: Run operator in the deferrable mode. This is only effective if wait_for_completion is
       set to True.
   :return Dict: Returns The ARN of the processing job created in Amazon SageMaker.

   .. py:method:: expand_role()

      Expand an IAM role name into an ARN.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)


   .. py:method:: get_openlineage_facets_on_complete(task_instance)

      Return OpenLineage data gathered from SageMaker's API response saved by processing job.



.. py:class:: SageMakerEndpointConfigOperator(*, config, aws_conn_id = DEFAULT_CONN_ID, **kwargs)


   Bases: :py:obj:`SageMakerBaseOperator`

   Creates an endpoint configuration that Amazon SageMaker hosting services uses to deploy models.

   In the configuration, you identify one or more models, created using the CreateModel API, to deploy and
   the resources that you want Amazon SageMaker to provision.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerEndpointConfigOperator`

   :param config: The configuration necessary to create an endpoint config.

       For details of the configuration parameter see :py:meth:`SageMaker.Client.create_endpoint_config`
   :param aws_conn_id: The AWS connection ID to use.
   :return Dict: Returns The ARN of the endpoint config created in Amazon SageMaker.

   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: SageMakerEndpointOperator(*, config, aws_conn_id = DEFAULT_CONN_ID, wait_for_completion = True, check_interval = CHECK_INTERVAL_SECOND, max_ingestion_time = None, operation = 'create', deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`SageMakerBaseOperator`

   When you create a serverless endpoint, SageMaker provisions and manages the compute resources for you.

   Then, you can make inference requests to the endpoint and receive model predictions
   in response. SageMaker scales the compute resources up and down as needed to handle
   your request traffic.

   Requires an Endpoint Config.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerEndpointOperator`

   :param config:
       The configuration necessary to create an endpoint.

       If you need to create a SageMaker endpoint based on an existed
       SageMaker model and an existed SageMaker endpoint config::

           config = endpoint_configuration;

       If you need to create all of SageMaker model, SageMaker endpoint-config and SageMaker endpoint::

           config = {
               'Model': model_configuration,
               'EndpointConfig': endpoint_config_configuration,
               'Endpoint': endpoint_configuration
           }

       For details of the configuration parameter of model_configuration see
       :py:meth:`SageMaker.Client.create_model`

       For details of the configuration parameter of endpoint_config_configuration see
       :py:meth:`SageMaker.Client.create_endpoint_config`

       For details of the configuration parameter of endpoint_configuration see
       :py:meth:`SageMaker.Client.create_endpoint`

   :param wait_for_completion: Whether the operator should wait until the endpoint creation finishes.
   :param check_interval: If wait is set to True, this is the time interval, in seconds, that this operation
       waits before polling the status of the endpoint creation.
   :param max_ingestion_time: If wait is set to True, this operation fails if the endpoint creation doesn't
       finish within max_ingestion_time seconds. If you set this parameter to None it never times out.
   :param operation: Whether to create an endpoint or update an endpoint. Must be either 'create or 'update'.
   :param aws_conn_id: The AWS connection ID to use.
   :param deferrable:  Will wait asynchronously for completion.
   :return Dict: Returns The ARN of the endpoint created in Amazon SageMaker.

   .. py:method:: expand_role()

      Expand an IAM role name into an ARN.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: SageMakerTransformOperator(*, config, aws_conn_id = DEFAULT_CONN_ID, wait_for_completion = True, check_interval = CHECK_INTERVAL_SECOND, max_attempts = None, max_ingestion_time = None, check_if_job_exists = True, action_if_job_exists = 'timestamp', deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`SageMakerBaseOperator`

   Starts a transform job.

   A transform job uses a trained model to get inferences on a dataset
   and saves these results to an Amazon S3 location that you specify.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerTransformOperator`

   :param config: The configuration necessary to start a transform job (templated).

       If you need to create a SageMaker transform job based on an existed SageMaker model::

           config = transform_config

       If you need to create both SageMaker model and SageMaker Transform job::

           config = {
               'Model': model_config,
               'Transform': transform_config
           }

       For details of the configuration parameter of transform_config see
       :py:meth:`SageMaker.Client.create_transform_job`

       For details of the configuration parameter of model_config, See:
       :py:meth:`SageMaker.Client.create_model`

   :param aws_conn_id: The AWS connection ID to use.
   :param wait_for_completion: Set to True to wait until the transform job finishes.
   :param check_interval: If wait is set to True, the time interval, in seconds,
       that this operation waits to check the status of the transform job.
   :param max_attempts: Number of times to poll for query state before returning the current state,
       defaults to None.
   :param max_ingestion_time: If wait is set to True, the operation fails
       if the transform job doesn't finish within max_ingestion_time seconds. If you
       set this parameter to None, the operation does not timeout.
   :param check_if_job_exists: If set to true, then the operator will check whether a transform job
       already exists for the name in the config.
   :param action_if_job_exists: Behaviour if the job name already exists. Possible options are "timestamp"
       (default), "increment" (deprecated) and "fail".
       This is only relevant if check_if_job_exists is True.
   :return Dict: Returns The ARN of the model created in Amazon SageMaker.

   .. py:method:: expand_role()

      Expand an IAM role name into an ARN.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)


   .. py:method:: get_openlineage_facets_on_complete(task_instance)

      Return OpenLineage data gathered from SageMaker's API response saved by transform job.



.. py:class:: SageMakerTuningOperator(*, config, aws_conn_id = DEFAULT_CONN_ID, wait_for_completion = True, check_interval = CHECK_INTERVAL_SECOND, max_ingestion_time = None, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`SageMakerBaseOperator`

   Starts a hyperparameter tuning job.

   A hyperparameter tuning job finds the best version of a model by running
   many training jobs on your dataset using the algorithm you choose and
   values for hyperparameters within ranges that you specify. It then chooses
   the hyperparameter values that result in a model that performs the best,
   as measured by an objective metric that you choose.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerTuningOperator`

   :param config: The configuration necessary to start a tuning job (templated).

       For details of the configuration parameter see
       :py:meth:`SageMaker.Client.create_hyper_parameter_tuning_job`
   :param aws_conn_id: The AWS connection ID to use.
   :param wait_for_completion: Set to True to wait until the tuning job finishes.
   :param check_interval: If wait is set to True, the time interval, in seconds,
       that this operation waits to check the status of the tuning job.
   :param max_ingestion_time: If wait is set to True, the operation fails
       if the tuning job doesn't finish within max_ingestion_time seconds. If you
       set this parameter to None, the operation does not timeout.
   :param deferrable: Will wait asynchronously for completion.
   :return Dict: Returns The ARN of the tuning job created in Amazon SageMaker.

   .. py:method:: expand_role()

      Expand an IAM role name into an ARN.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: SageMakerModelOperator(*, config, aws_conn_id = DEFAULT_CONN_ID, **kwargs)


   Bases: :py:obj:`SageMakerBaseOperator`

   Creates a model in Amazon SageMaker.

   In the request, you name the model and describe a primary container. For the
   primary container, you specify the Docker image that contains inference code,
   artifacts (from prior training), and a custom environment map that the inference
   code uses when you deploy the model for predictions.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerModelOperator`

   :param config: The configuration necessary to create a model.

       For details of the configuration parameter see :py:meth:`SageMaker.Client.create_model`
   :param aws_conn_id: The AWS connection ID to use.
   :return Dict: Returns The ARN of the model created in Amazon SageMaker.

   .. py:method:: expand_role()

      Expand an IAM role name into an ARN.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: SageMakerTrainingOperator(*, config, aws_conn_id = DEFAULT_CONN_ID, wait_for_completion = True, print_log = True, check_interval = CHECK_INTERVAL_SECOND, max_attempts = None, max_ingestion_time = None, check_if_job_exists = True, action_if_job_exists = 'timestamp', deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`SageMakerBaseOperator`

   Starts a model training job.

   After training completes, Amazon SageMaker saves the resulting
   model artifacts to an Amazon S3 location that you specify.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerTrainingOperator`

   :param config: The configuration necessary to start a training job (templated).

       For details of the configuration parameter see :py:meth:`SageMaker.Client.create_training_job`
   :param aws_conn_id: The AWS connection ID to use.
   :param wait_for_completion: If wait is set to True, the time interval, in seconds,
       that the operation waits to check the status of the training job.
   :param print_log: if the operator should print the cloudwatch log during training
   :param check_interval: if wait is set to be true, this is the time interval
       in seconds which the operator will check the status of the training job
   :param max_attempts: Number of times to poll for query state before returning the current state,
       defaults to None.
   :param max_ingestion_time: If wait is set to True, the operation fails if the training job
       doesn't finish within max_ingestion_time seconds. If you set this parameter to None,
       the operation does not timeout.
   :param check_if_job_exists: If set to true, then the operator will check whether a training job
       already exists for the name in the config.
   :param action_if_job_exists: Behaviour if the job name already exists. Possible options are "timestamp"
       (default), "increment" (deprecated) and "fail".
       This is only relevant if check_if_job_exists is True.
   :param deferrable: Run operator in the deferrable mode. This is only effective if wait_for_completion is
       set to True.
   :return Dict: Returns The ARN of the training job created in Amazon SageMaker.

   .. py:method:: expand_role()

      Expand an IAM role name into an ARN.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)


   .. py:method:: get_openlineage_facets_on_complete(task_instance)

      Return OpenLineage data gathered from SageMaker's API response saved by training job.



.. py:class:: SageMakerDeleteModelOperator(*, config, aws_conn_id = DEFAULT_CONN_ID, **kwargs)


   Bases: :py:obj:`SageMakerBaseOperator`

   Deletes a SageMaker model.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerDeleteModelOperator`

   :param config: The configuration necessary to delete the model.
       For details of the configuration parameter see :py:meth:`SageMaker.Client.delete_model`
   :param aws_conn_id: The AWS connection ID to use.

   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: SageMakerStartPipelineOperator(*, aws_conn_id = DEFAULT_CONN_ID, pipeline_name, display_name = 'airflow-triggered-execution', pipeline_params = None, wait_for_completion = False, check_interval = CHECK_INTERVAL_SECOND, waiter_max_attempts = 9999, verbose = True, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`SageMakerBaseOperator`

   Starts a SageMaker pipeline execution.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerStartPipelineOperator`

   :param config: The configuration to start the pipeline execution.
   :param aws_conn_id: The AWS connection ID to use.
   :param pipeline_name: Name of the pipeline to start.
   :param display_name: The name this pipeline execution will have in the UI. Doesn't need to be unique.
   :param pipeline_params: Optional parameters for the pipeline.
       All parameters supplied need to already be present in the pipeline definition.
   :param wait_for_completion: If true, this operator will only complete once the pipeline is complete.
   :param check_interval: How long to wait between checks for pipeline status when waiting for completion.
   :param waiter_max_attempts: How many times to check the status before failing.
   :param verbose: Whether to print steps details when waiting for completion.
       Defaults to true, consider turning off for pipelines that have thousands of steps.
   :param deferrable: Run operator in the deferrable mode.

   :return str: Returns The ARN of the pipeline execution created in Amazon SageMaker.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('aws_conn_id', 'pipeline_name', 'display_name', 'pipeline_params')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event = None)



.. py:class:: SageMakerStopPipelineOperator(*, aws_conn_id = DEFAULT_CONN_ID, pipeline_exec_arn, wait_for_completion = False, check_interval = CHECK_INTERVAL_SECOND, waiter_max_attempts = 9999, verbose = True, fail_if_not_running = False, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`SageMakerBaseOperator`

   Stops a SageMaker pipeline execution.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerStopPipelineOperator`

   :param config: The configuration to start the pipeline execution.
   :param aws_conn_id: The AWS connection ID to use.
   :param pipeline_exec_arn: Amazon Resource Name of the pipeline execution to stop.
   :param wait_for_completion: If true, this operator will only complete once the pipeline is fully stopped.
   :param check_interval: How long to wait between checks for pipeline status when waiting for completion.
   :param verbose: Whether to print steps details when waiting for completion.
       Defaults to true, consider turning off for pipelines that have thousands of steps.
   :param fail_if_not_running: raises an exception if the pipeline stopped or succeeded before this was run
   :param deferrable: Run operator in the deferrable mode.

   :return str: Returns the status of the pipeline execution after the operation has been done.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('aws_conn_id', 'pipeline_exec_arn')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event = None)



.. py:class:: SageMakerRegisterModelVersionOperator(*, image_uri, model_url, package_group_name, package_group_desc = '', package_desc = '', model_approval = ApprovalStatus.PENDING_MANUAL_APPROVAL, extras = None, aws_conn_id = DEFAULT_CONN_ID, config = None, **kwargs)


   Bases: :py:obj:`SageMakerBaseOperator`

   Register a SageMaker model by creating a model version that specifies the model group to which it belongs.

   Will create the model group if it does not exist already.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerRegisterModelVersionOperator`

   :param image_uri: The Amazon EC2 Container Registry (Amazon ECR) path where inference code is stored.
   :param model_url: The Amazon S3 path where the model artifacts (the trained weights of the model), which
       result from model training, are stored. This path must point to a single gzip compressed tar archive
       (.tar.gz suffix).
   :param package_group_name: The name of the model package group that the model is going to be registered
       to. Will be created if it doesn't already exist.
   :param package_group_desc: Description of the model package group, if it was to be created (optional).
   :param package_desc: Description of the model package (optional).
   :param model_approval: Approval status of the model package. Defaults to PendingManualApproval
   :param extras: Can contain extra parameters for the boto call to create_model_package, and/or overrides
       for any parameter defined above. For a complete list of available parameters, see
       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_model_package

   :return str: Returns the ARN of the model package created.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('image_uri', 'model_url', 'package_group_name', 'package_group_desc', 'package_desc', 'model_approval')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: SageMakerAutoMLOperator(*, job_name, s3_input, target_attribute, s3_output, role_arn, compressed_input = False, time_limit = None, autodeploy_endpoint_name = None, extras = None, wait_for_completion = True, check_interval = 30, aws_conn_id = DEFAULT_CONN_ID, config = None, **kwargs)


   Bases: :py:obj:`SageMakerBaseOperator`

   Creates an auto ML job, learning to predict the given column from the data provided through S3.

   The learning output is written to the specified S3 location.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerAutoMLOperator`

   :param job_name: Name of the job to create, needs to be unique within the account.
   :param s3_input: The S3 location (folder or file) where to fetch the data.
       By default, it expects csv with headers.
   :param target_attribute: The name of the column containing the values to predict.
   :param s3_output: The S3 folder where to write the model artifacts. Must be 128 characters or fewer.
   :param role_arn: The ARN of the IAM role to use when interacting with S3.
       Must have read access to the input, and write access to the output folder.
   :param compressed_input: Set to True if the input is gzipped.
   :param time_limit: The maximum amount of time in seconds to spend training the model(s).
   :param autodeploy_endpoint_name: If specified, the best model will be deployed to an endpoint with
       that name. No deployment made otherwise.
   :param extras: Use this dictionary to set any variable input variable for job creation that is not
       offered through the parameters of this function. The format is described in:
       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_auto_ml_job
   :param wait_for_completion: Whether to wait for the job to finish before returning. Defaults to True.
   :param check_interval: Interval in seconds between 2 status checks when waiting for completion.

   :returns: Only if waiting for completion, a dictionary detailing the best model. The structure is that of
       the "BestCandidate" key in:
       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_auto_ml_job

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_name', 's3_input', 'target_attribute', 's3_output', 'role_arn', 'compressed_input',...



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: SageMakerCreateExperimentOperator(*, name, description = None, tags = None, aws_conn_id = DEFAULT_CONN_ID, **kwargs)


   Bases: :py:obj:`SageMakerBaseOperator`

   Creates a SageMaker experiment, to be then associated to jobs etc.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerCreateExperimentOperator`

   :param name: name of the experiment, must be unique within the AWS account
   :param description: description of the experiment, optional
   :param tags: tags to attach to the experiment, optional
   :param aws_conn_id: The AWS connection ID to use.

   :returns: the ARN of the experiment created, though experiments are referred to by name

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('name', 'description', 'tags')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: SageMakerCreateNotebookOperator(*, instance_name, instance_type, role_arn, volume_size_in_gb = None, volume_kms_key_id = None, lifecycle_config_name = None, direct_internet_access = None, root_access = None, create_instance_kwargs = {}, wait_for_completion = True, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Create a SageMaker notebook.

   More information regarding parameters of this operator can be found here
   https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker/client/create_notebook_instance.html.

   .. seealso:
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerCreateNotebookOperator`

   :param instance_name: The name of the notebook instance.
   :param instance_type: The type of instance to create.
   :param role_arn: The Amazon Resource Name (ARN) of the IAM role that SageMaker can assume to access
   :param volume_size_in_gb: Size in GB of the EBS root device volume of the notebook instance.
   :param volume_kms_key_id: The KMS key ID for the EBS root device volume.
   :param lifecycle_config_name: The name of the lifecycle configuration to associate with the notebook
   :param direct_internet_access: Whether to enable direct internet access for the notebook instance.
   :param root_access: Whether to give the notebook instance root access to the Amazon S3 bucket.
   :param wait_for_completion: Whether or not to wait for the notebook to be InService before returning
   :param create_instance_kwargs: Additional configuration options for the create call.
   :param aws_conn_id: The AWS connection ID to use.

   :return: The ARN of the created notebook.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('instance_name', 'instance_type', 'role_arn', 'volume_size_in_gb', 'volume_kms_key_id',...



   .. py:attribute:: ui_color
      :value: '#ff7300'



   .. py:method:: hook()

      Create and return SageMakerHook.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: SageMakerStopNotebookOperator(instance_name, wait_for_completion = True, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Stop a notebook instance.

   .. seealso:
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerStopNotebookOperator`

   :param instance_name: The name of the notebook instance to stop.
   :param wait_for_completion: Whether or not to wait for the notebook to be stopped before returning
   :param aws_conn_id: The AWS connection ID to use.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('instance_name', 'wait_for_completion')



   .. py:attribute:: ui_color
      :value: '#ff7300'



   .. py:method:: hook()

      Create and return SageMakerHook.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: SageMakerDeleteNotebookOperator(instance_name, wait_for_completion = True, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Delete a notebook instance.

   .. seealso:
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerDeleteNotebookOperator`

   :param instance_name: The name of the notebook instance to delete.
   :param wait_for_completion: Whether or not to wait for the notebook to delete before returning.
   :param aws_conn_id: The AWS connection ID to use.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('instance_name', 'wait_for_completion')



   .. py:attribute:: ui_color
      :value: '#ff7300'



   .. py:method:: hook()

      Create and return SageMakerHook.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: SageMakerStartNoteBookOperator(instance_name, wait_for_completion = True, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Start a notebook instance.

   .. seealso:
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SageMakerStartNotebookOperator`

   :param instance_name: The name of the notebook instance to start.
   :param wait_for_completion: Whether or not to wait for notebook to be InService before returning
   :param aws_conn_id: The AWS connection ID to use.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('instance_name', 'wait_for_completion')



   .. py:attribute:: ui_color
      :value: '#ff7300'



   .. py:method:: hook()

      Create and return SageMakerHook.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
