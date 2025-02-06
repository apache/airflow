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

:py:mod:`airflow.providers.amazon.aws.hooks.sagemaker`
======================================================

.. py:module:: airflow.providers.amazon.aws.hooks.sagemaker


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.sagemaker.LogState
   airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook



Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.sagemaker.argmin
   airflow.providers.amazon.aws.hooks.sagemaker.secondary_training_status_changed
   airflow.providers.amazon.aws.hooks.sagemaker.secondary_training_status_message



Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.sagemaker.Position


.. py:class:: LogState


   Enum-style class holding all possible states of CloudWatch log streams.

   https://sagemaker.readthedocs.io/en/stable/session.html#sagemaker.session.LogState

   .. py:attribute:: STARTING
      :value: 1



   .. py:attribute:: WAIT_IN_PROGRESS
      :value: 2



   .. py:attribute:: TAILING
      :value: 3



   .. py:attribute:: JOB_COMPLETE
      :value: 4



   .. py:attribute:: COMPLETE
      :value: 5




.. py:data:: Position



.. py:function:: argmin(arr, f)

   Given callable ``f``, find index in ``arr`` to minimize ``f(arr[i])``.

   None is returned if ``arr`` is empty.


.. py:function:: secondary_training_status_changed(current_job_description, prev_job_description)

   Check if training job's secondary status message has changed.

   :param current_job_description: Current job description, returned from DescribeTrainingJob call.
   :param prev_job_description: Previous job description, returned from DescribeTrainingJob call.

   :return: Whether the secondary status message of a training job changed or not.


.. py:function:: secondary_training_status_message(job_description, prev_description)

   Format string containing start time and the secondary training job status message.

   :param job_description: Returned response from DescribeTrainingJob call
   :param prev_description: Previous job description from DescribeTrainingJob call

   :return: Job status string to be printed.


.. py:class:: SageMakerHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon SageMaker.

   Provide thick wrapper around
   :external+boto3:py:class:`boto3.client("sagemaker") <SageMaker.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:attribute:: non_terminal_states



   .. py:attribute:: endpoint_non_terminal_states



   .. py:attribute:: pipeline_non_terminal_states



   .. py:attribute:: failed_states



   .. py:method:: tar_and_s3_upload(path, key, bucket)

      Tar the local file or directory and upload to s3.

      :param path: local file or directory
      :param key: s3 key
      :param bucket: s3 bucket


   .. py:method:: configure_s3_resources(config)

      Extract the S3 operations from the configuration and execute them.

      :param config: config of SageMaker operation


   .. py:method:: check_s3_url(s3url)

      Check if an S3 URL exists.

      :param s3url: S3 url


   .. py:method:: check_training_config(training_config)

      Check if a training configuration is valid.

      :param training_config: training_config


   .. py:method:: check_tuning_config(tuning_config)

      Check if a tuning configuration is valid.

      :param tuning_config: tuning_config


   .. py:method:: multi_stream_iter(log_group, streams, positions=None)

      Iterate over the available events.

      The events coming from a set of log streams in a single log group
      interleaving the events from each stream so they're yielded in timestamp order.

      :param log_group: The name of the log group.
      :param streams: A list of the log stream names. The position of the stream in this list is
          the stream number.
      :param positions: A list of pairs of (timestamp, skip) which represents the last record
          read from each stream.
      :return: A tuple of (stream number, cloudwatch log event).


   .. py:method:: create_training_job(config, wait_for_completion = True, print_log = True, check_interval = 30, max_ingestion_time = None)

      Start a model training job.

      After training completes, Amazon SageMaker saves the resulting model
      artifacts to an Amazon S3 location that you specify.

      :param config: the config for training
      :param wait_for_completion: if the program should keep running until job finishes
      :param check_interval: the time interval in seconds which the operator
          will check the status of any SageMaker job
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.
      :return: A response to training job creation


   .. py:method:: create_tuning_job(config, wait_for_completion = True, check_interval = 30, max_ingestion_time = None)

      Start a hyperparameter tuning job.

      A hyperparameter tuning job finds the best version of a model by running
      many training jobs on your dataset using the algorithm you choose and
      values for hyperparameters within ranges that you specify. It then
      chooses the hyperparameter values that result in a model that performs
      the best, as measured by an objective metric that you choose.

      :param config: the config for tuning
      :param wait_for_completion: if the program should keep running until job finishes
      :param check_interval: the time interval in seconds which the operator
          will check the status of any SageMaker job
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.
      :return: A response to tuning job creation


   .. py:method:: create_transform_job(config, wait_for_completion = True, check_interval = 30, max_ingestion_time = None)

      Start a transform job.

      A transform job uses a trained model to get inferences on a dataset and
      saves these results to an Amazon S3 location that you specify.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.create_transform_job`

      :param config: the config for transform job
      :param wait_for_completion: if the program should keep running until job finishes
      :param check_interval: the time interval in seconds which the operator
          will check the status of any SageMaker job
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.
      :return: A response to transform job creation


   .. py:method:: create_processing_job(config, wait_for_completion = True, check_interval = 30, max_ingestion_time = None)

      Use Amazon SageMaker Processing to analyze data and evaluate models.

      With Processing, you can use a simplified, managed experience on
      SageMaker to run your data processing workloads, such as feature
      engineering, data validation, model evaluation, and model
      interpretation.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.create_processing_job`

      :param config: the config for processing job
      :param wait_for_completion: if the program should keep running until job finishes
      :param check_interval: the time interval in seconds which the operator
          will check the status of any SageMaker job
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.
      :return: A response to transform job creation


   .. py:method:: create_model(config)

      Create a model in Amazon SageMaker.

      In the request, you name the model and describe a primary container. For
      the primary container, you specify the Docker image that contains
      inference code, artifacts (from prior training), and a custom
      environment map that the inference code uses when you deploy the model
      for predictions.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.create_model`

      :param config: the config for model
      :return: A response to model creation


   .. py:method:: create_endpoint_config(config)

      Create an endpoint configuration to deploy models.

      In the configuration, you identify one or more models, created using the
      CreateModel API, to deploy and the resources that you want Amazon
      SageMaker to provision.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.create_endpoint_config`
          - :class:`airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.create_model`
          - :class:`airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.create_endpoint`

      :param config: the config for endpoint-config
      :return: A response to endpoint config creation


   .. py:method:: create_endpoint(config, wait_for_completion = True, check_interval = 30, max_ingestion_time = None)

      Create an endpoint from configuration.

      When you create a serverless endpoint, SageMaker provisions and manages
      the compute resources for you. Then, you can make inference requests to
      the endpoint and receive model predictions in response. SageMaker scales
      the compute resources up and down as needed to handle your request traffic.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.create_endpoint`
          - :class:`airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.create_endpoint`

      :param config: the config for endpoint
      :param wait_for_completion: if the program should keep running until job finishes
      :param check_interval: the time interval in seconds which the operator
          will check the status of any SageMaker job
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.
      :return: A response to endpoint creation


   .. py:method:: update_endpoint(config, wait_for_completion = True, check_interval = 30, max_ingestion_time = None)

      Deploy the config in the request and switch to using the new endpoint.

      Resources provisioned for the endpoint using the previous EndpointConfig
      are deleted (there is no availability loss).

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.update_endpoint`

      :param config: the config for endpoint
      :param wait_for_completion: if the program should keep running until job finishes
      :param check_interval: the time interval in seconds which the operator
          will check the status of any SageMaker job
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.
      :return: A response to endpoint update


   .. py:method:: describe_training_job(name)

      Get the training job info associated with the name.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.describe_training_job`

      :param name: the name of the training job
      :return: A dict contains all the training job info


   .. py:method:: describe_training_job_with_log(job_name, positions, stream_names, instance_count, state, last_description, last_describe_job_call)

      Get the associated training job info and print CloudWatch logs.


   .. py:method:: describe_tuning_job(name)

      Get the tuning job info associated with the name.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.describe_hyper_parameter_tuning_job`

      :param name: the name of the tuning job
      :return: A dict contains all the tuning job info


   .. py:method:: describe_model(name)

      Get the SageMaker model info associated with the name.

      :param name: the name of the SageMaker model
      :return: A dict contains all the model info


   .. py:method:: describe_transform_job(name)

      Get the transform job info associated with the name.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.describe_transform_job`

      :param name: the name of the transform job
      :return: A dict contains all the transform job info


   .. py:method:: describe_processing_job(name)

      Get the processing job info associated with the name.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.describe_processing_job`

      :param name: the name of the processing job
      :return: A dict contains all the processing job info


   .. py:method:: describe_endpoint_config(name)

      Get the endpoint config info associated with the name.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.describe_endpoint_config`

      :param name: the name of the endpoint config
      :return: A dict contains all the endpoint config info


   .. py:method:: describe_endpoint(name)

      Get the description of an endpoint.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.describe_endpoint`

      :param name: the name of the endpoint
      :return: A dict contains all the endpoint info


   .. py:method:: check_status(job_name, key, describe_function, check_interval, max_ingestion_time = None, non_terminal_states = None)

      Check status of a SageMaker resource.

      :param job_name: name of the resource to check status, can be a job but
          also pipeline for instance.
      :param key: the key of the response dict that points to the state
      :param describe_function: the function used to retrieve the status
      :param args: the arguments for the function
      :param check_interval: the time interval in seconds which the operator
          will check the status of any SageMaker resource
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker resources that run longer than this will fail. Setting
          this to None implies no timeout for any SageMaker resource.
      :param non_terminal_states: the set of nonterminal states
      :return: response of describe call after resource is done


   .. py:method:: check_training_status_with_log(job_name, non_terminal_states, failed_states, wait_for_completion, check_interval, max_ingestion_time = None)

      Display logs for a given training job.

      Optionally tailing them until the job is complete.

      :param job_name: name of the training job to check status and display logs for
      :param non_terminal_states: the set of non_terminal states
      :param failed_states: the set of failed states
      :param wait_for_completion: Whether to keep looking for new log entries
          until the job completes
      :param check_interval: The interval in seconds between polling for new log entries and job completion
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.


   .. py:method:: list_training_jobs(name_contains = None, max_results = None, **kwargs)

      Call boto3's ``list_training_jobs``.

      The training job name and max results are configurable via arguments.
      Other arguments are not, and should be provided via kwargs. Note that
      boto3 expects these in CamelCase, for example:

      .. code-block:: python

          list_training_jobs(name_contains="myjob", StatusEquals="Failed")

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.list_training_jobs`

      :param name_contains: (optional) partial name to match
      :param max_results: (optional) maximum number of results to return. None returns infinite results
      :param kwargs: (optional) kwargs to boto3's list_training_jobs method
      :return: results of the list_training_jobs request


   .. py:method:: list_transform_jobs(name_contains = None, max_results = None, **kwargs)

      Call boto3's ``list_transform_jobs``.

      The transform job name and max results are configurable via arguments.
      Other arguments are not, and should be provided via kwargs. Note that
      boto3 expects these in CamelCase, for example:

      .. code-block:: python

          list_transform_jobs(name_contains="myjob", StatusEquals="Failed")

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.list_transform_jobs`

      :param name_contains: (optional) partial name to match.
      :param max_results: (optional) maximum number of results to return.
          None returns infinite results.
      :param kwargs: (optional) kwargs to boto3's list_transform_jobs method.
      :return: results of the list_transform_jobs request.


   .. py:method:: list_processing_jobs(**kwargs)

      Call boto3's `list_processing_jobs`.

      All arguments should be provided via kwargs. Note that boto3 expects
      these in CamelCase, for example:

      .. code-block:: python

          list_processing_jobs(NameContains="myjob", StatusEquals="Failed")

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.list_processing_jobs`

      :param kwargs: (optional) kwargs to boto3's list_training_jobs method
      :return: results of the list_processing_jobs request


   .. py:method:: count_processing_jobs_by_name(processing_job_name, job_name_suffix = None, throttle_retry_delay = 2, retries = 3)

      Get the number of processing jobs found with the provided name prefix.

      :param processing_job_name: The prefix to look for.
      :param job_name_suffix: The optional suffix which may be appended to deduplicate an existing job name.
      :param throttle_retry_delay: Seconds to wait if a ThrottlingException is hit.
      :param retries: The max number of times to retry.
      :returns: The number of processing jobs that start with the provided prefix.


   .. py:method:: delete_model(model_name)

      Delete a SageMaker model.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.delete_model`

      :param model_name: name of the model


   .. py:method:: describe_pipeline_exec(pipeline_exec_arn, verbose = False)

      Get info about a SageMaker pipeline execution.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.describe_pipeline_execution`
          - :external+boto3:py:meth:`SageMaker.Client.list_pipeline_execution_steps`

      :param pipeline_exec_arn: arn of the pipeline execution
      :param verbose: Whether to log details about the steps status in the pipeline execution


   .. py:method:: start_pipeline(pipeline_name, display_name = 'airflow-triggered-execution', pipeline_params = None, wait_for_completion = False, check_interval = None, verbose = True)

      Start a new execution for a SageMaker pipeline.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.start_pipeline_execution`

      :param pipeline_name: Name of the pipeline to start (this is _not_ the ARN).
      :param display_name: The name this pipeline execution will have in the UI. Doesn't need to be unique.
      :param pipeline_params: Optional parameters for the pipeline.
          All parameters supplied need to already be present in the pipeline definition.

      :return: the ARN of the pipeline execution launched.


   .. py:method:: stop_pipeline(pipeline_exec_arn, wait_for_completion = False, check_interval = None, verbose = True, fail_if_not_running = False)

      Stop SageMaker pipeline execution.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.stop_pipeline_execution`

      :param pipeline_exec_arn: Amazon Resource Name (ARN) of the pipeline execution.
          It's the ARN of the pipeline itself followed by "/execution/" and an id.
      :param fail_if_not_running: This method will raise an exception if the pipeline we're trying to stop
          is not in an "Executing" state when the call is sent (which would mean that the pipeline is
          already either stopping or stopped).
          Note that setting this to True will raise an error if the pipeline finished successfully before it
          was stopped.
      :return: Status of the pipeline execution after the operation.
          One of 'Executing'|'Stopping'|'Stopped'|'Failed'|'Succeeded'.


   .. py:method:: create_model_package_group(package_group_name, package_group_desc = '')

      Create a Model Package Group if it does not already exist.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.create_model_package_group`

      :param package_group_name: Name of the model package group to create if not already present.
      :param package_group_desc: Description of the model package group, if it was to be created (optional).

      :return: True if the model package group was created, False if it already existed.


   .. py:method:: create_auto_ml_job(job_name, s3_input, target_attribute, s3_output, role_arn, compressed_input = False, time_limit = None, autodeploy_endpoint_name = None, extras = None, wait_for_completion = True, check_interval = 30)

      Create an auto ML job to predict the given column.

      The learning input is based on data provided through S3 , and the output
      is written to the specified S3 location.

      .. seealso::
          - :external+boto3:py:meth:`SageMaker.Client.create_auto_ml_job`

      :param job_name: Name of the job to create, needs to be unique within the account.
      :param s3_input: The S3 location (folder or file) where to fetch the data.
          By default, it expects csv with headers.
      :param target_attribute: The name of the column containing the values to predict.
      :param s3_output: The S3 folder where to write the model artifacts. Must be 128 characters or fewer.
      :param role_arn: The ARN or the IAM role to use when interacting with S3.
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

      :returns: Only if waiting for completion, a dictionary detailing the best model. The structure is that
          of the "BestCandidate" key in:
          https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_auto_ml_job
