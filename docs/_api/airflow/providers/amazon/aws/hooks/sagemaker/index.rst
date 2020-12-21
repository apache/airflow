:mod:`airflow.providers.amazon.aws.hooks.sagemaker`
===================================================

.. py:module:: airflow.providers.amazon.aws.hooks.sagemaker


Module Contents
---------------

.. py:class:: LogState

   Enum-style class holding all possible states of CloudWatch log streams.
   https://sagemaker.readthedocs.io/en/stable/session.html#sagemaker.session.LogState

   .. attribute:: STARTING
      :annotation: = 1

      

   .. attribute:: WAIT_IN_PROGRESS
      :annotation: = 2

      

   .. attribute:: TAILING
      :annotation: = 3

      

   .. attribute:: JOB_COMPLETE
      :annotation: = 4

      

   .. attribute:: COMPLETE
      :annotation: = 5

      


.. data:: Position
   

   

.. function:: argmin(arr, f: Callable) -> Optional[int]
   Return the index, i, in arr that minimizes f(arr[i])


.. function:: secondary_training_status_changed(current_job_description: dict, prev_job_description: dict) -> bool
   Returns true if training job's secondary status message has changed.

   :param current_job_description: Current job description, returned from DescribeTrainingJob call.
   :type current_job_description: dict
   :param prev_job_description: Previous job description, returned from DescribeTrainingJob call.
   :type prev_job_description: dict

   :return: Whether the secondary status message of a training job changed or not.


.. function:: secondary_training_status_message(job_description: Dict[str, List[dict]], prev_description: Optional[dict]) -> str
   Returns a string contains start time and the secondary training job status message.

   :param job_description: Returned response from DescribeTrainingJob call
   :type job_description: dict
   :param prev_description: Previous job description from DescribeTrainingJob call
   :type prev_description: dict

   :return: Job status string to be printed.


.. py:class:: SageMakerHook(*args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon SageMaker.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. attribute:: non_terminal_states
      

      

   .. attribute:: endpoint_non_terminal_states
      

      

   .. attribute:: failed_states
      

      

   
   .. method:: tar_and_s3_upload(self, path: str, key: str, bucket: str)

      Tar the local file or directory and upload to s3

      :param path: local file or directory
      :type path: str
      :param key: s3 key
      :type key: str
      :param bucket: s3 bucket
      :type bucket: str
      :return: None



   
   .. method:: configure_s3_resources(self, config: dict)

      Extract the S3 operations from the configuration and execute them.

      :param config: config of SageMaker operation
      :type config: dict
      :rtype: dict



   
   .. method:: check_s3_url(self, s3url: str)

      Check if an S3 URL exists

      :param s3url: S3 url
      :type s3url: str
      :rtype: bool



   
   .. method:: check_training_config(self, training_config: dict)

      Check if a training configuration is valid

      :param training_config: training_config
      :type training_config: dict
      :return: None



   
   .. method:: check_tuning_config(self, tuning_config: dict)

      Check if a tuning configuration is valid

      :param tuning_config: tuning_config
      :type tuning_config: dict
      :return: None



   
   .. method:: get_log_conn(self)

      This method is deprecated.
      Please use :py:meth:`airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_conn` instead.



   
   .. method:: log_stream(self, log_group, stream_name, start_time=0, skip=0)

      This method is deprecated.
      Please use
      :py:meth:`airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events` instead.



   
   .. method:: multi_stream_iter(self, log_group: str, streams: list, positions=None)

      Iterate over the available events coming from a set of log streams in a single log group
      interleaving the events from each stream so they're yielded in timestamp order.

      :param log_group: The name of the log group.
      :type log_group: str
      :param streams: A list of the log stream names. The position of the stream in this list is
          the stream number.
      :type streams: list
      :param positions: A list of pairs of (timestamp, skip) which represents the last record
          read from each stream.
      :type positions: list
      :return: A tuple of (stream number, cloudwatch log event).



   
   .. method:: create_training_job(self, config: dict, wait_for_completion: bool = True, print_log: bool = True, check_interval: int = 30, max_ingestion_time: Optional[int] = None)

      Create a training job

      :param config: the config for training
      :type config: dict
      :param wait_for_completion: if the program should keep running until job finishes
      :type wait_for_completion: bool
      :param check_interval: the time interval in seconds which the operator
          will check the status of any SageMaker job
      :type check_interval: int
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.
      :type max_ingestion_time: int
      :return: A response to training job creation



   
   .. method:: create_tuning_job(self, config: dict, wait_for_completion: bool = True, check_interval: int = 30, max_ingestion_time: Optional[int] = None)

      Create a tuning job

      :param config: the config for tuning
      :type config: dict
      :param wait_for_completion: if the program should keep running until job finishes
      :type wait_for_completion: bool
      :param check_interval: the time interval in seconds which the operator
          will check the status of any SageMaker job
      :type check_interval: int
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.
      :type max_ingestion_time: int
      :return: A response to tuning job creation



   
   .. method:: create_transform_job(self, config: dict, wait_for_completion: bool = True, check_interval: int = 30, max_ingestion_time: Optional[int] = None)

      Create a transform job

      :param config: the config for transform job
      :type config: dict
      :param wait_for_completion: if the program should keep running until job finishes
      :type wait_for_completion: bool
      :param check_interval: the time interval in seconds which the operator
          will check the status of any SageMaker job
      :type check_interval: int
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.
      :type max_ingestion_time: int
      :return: A response to transform job creation



   
   .. method:: create_processing_job(self, config: dict, wait_for_completion: bool = True, check_interval: int = 30, max_ingestion_time: Optional[int] = None)

      Create a processing job

      :param config: the config for processing job
      :type config: dict
      :param wait_for_completion: if the program should keep running until job finishes
      :type wait_for_completion: bool
      :param check_interval: the time interval in seconds which the operator
          will check the status of any SageMaker job
      :type check_interval: int
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.
      :type max_ingestion_time: int
      :return: A response to transform job creation



   
   .. method:: create_model(self, config: dict)

      Create a model job

      :param config: the config for model
      :type config: dict
      :return: A response to model creation



   
   .. method:: create_endpoint_config(self, config: dict)

      Create an endpoint config

      :param config: the config for endpoint-config
      :type config: dict
      :return: A response to endpoint config creation



   
   .. method:: create_endpoint(self, config: dict, wait_for_completion: bool = True, check_interval: int = 30, max_ingestion_time: Optional[int] = None)

      Create an endpoint

      :param config: the config for endpoint
      :type config: dict
      :param wait_for_completion: if the program should keep running until job finishes
      :type wait_for_completion: bool
      :param check_interval: the time interval in seconds which the operator
          will check the status of any SageMaker job
      :type check_interval: int
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.
      :type max_ingestion_time: int
      :return: A response to endpoint creation



   
   .. method:: update_endpoint(self, config: dict, wait_for_completion: bool = True, check_interval: int = 30, max_ingestion_time: Optional[int] = None)

      Update an endpoint

      :param config: the config for endpoint
      :type config: dict
      :param wait_for_completion: if the program should keep running until job finishes
      :type wait_for_completion: bool
      :param check_interval: the time interval in seconds which the operator
          will check the status of any SageMaker job
      :type check_interval: int
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.
      :type max_ingestion_time: int
      :return: A response to endpoint update



   
   .. method:: describe_training_job(self, name: str)

      Return the training job info associated with the name

      :param name: the name of the training job
      :type name: str
      :return: A dict contains all the training job info



   
   .. method:: describe_training_job_with_log(self, job_name: str, positions, stream_names: list, instance_count: int, state: int, last_description: dict, last_describe_job_call: float)

      Return the training job info associated with job_name and print CloudWatch logs



   
   .. method:: describe_tuning_job(self, name: str)

      Return the tuning job info associated with the name

      :param name: the name of the tuning job
      :type name: str
      :return: A dict contains all the tuning job info



   
   .. method:: describe_model(self, name: str)

      Return the SageMaker model info associated with the name

      :param name: the name of the SageMaker model
      :type name: str
      :return: A dict contains all the model info



   
   .. method:: describe_transform_job(self, name: str)

      Return the transform job info associated with the name

      :param name: the name of the transform job
      :type name: str
      :return: A dict contains all the transform job info



   
   .. method:: describe_processing_job(self, name: str)

      Return the processing job info associated with the name

      :param name: the name of the processing job
      :type name: str
      :return: A dict contains all the processing job info



   
   .. method:: describe_endpoint_config(self, name: str)

      Return the endpoint config info associated with the name

      :param name: the name of the endpoint config
      :type name: str
      :return: A dict contains all the endpoint config info



   
   .. method:: describe_endpoint(self, name: str)

      :param name: the name of the endpoint
      :type name: str
      :return: A dict contains all the endpoint info



   
   .. method:: check_status(self, job_name: str, key: str, describe_function: Callable, check_interval: int, max_ingestion_time: Optional[int] = None, non_terminal_states: Optional[Set] = None)

      Check status of a SageMaker job

      :param job_name: name of the job to check status
      :type job_name: str
      :param key: the key of the response dict
          that points to the state
      :type key: str
      :param describe_function: the function used to retrieve the status
      :type describe_function: python callable
      :param args: the arguments for the function
      :param check_interval: the time interval in seconds which the operator
          will check the status of any SageMaker job
      :type check_interval: int
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.
      :type max_ingestion_time: int
      :param non_terminal_states: the set of nonterminal states
      :type non_terminal_states: set
      :return: response of describe call after job is done



   
   .. method:: check_training_status_with_log(self, job_name: str, non_terminal_states: set, failed_states: set, wait_for_completion: bool, check_interval: int, max_ingestion_time: Optional[int] = None)

      Display the logs for a given training job, optionally tailing them until the
      job is complete.

      :param job_name: name of the training job to check status and display logs for
      :type job_name: str
      :param non_terminal_states: the set of non_terminal states
      :type non_terminal_states: set
      :param failed_states: the set of failed states
      :type failed_states: set
      :param wait_for_completion: Whether to keep looking for new log entries
          until the job completes
      :type wait_for_completion: bool
      :param check_interval: The interval in seconds between polling for new log entries and job completion
      :type check_interval: int
      :param max_ingestion_time: the maximum ingestion time in seconds. Any
          SageMaker jobs that run longer than this will fail. Setting this to
          None implies no timeout for any SageMaker job.
      :type max_ingestion_time: int
      :return: None



   
   .. method:: list_training_jobs(self, name_contains: Optional[str] = None, max_results: Optional[int] = None, **kwargs)

      This method wraps boto3's list_training_jobs(). The training job name and max results are configurable
      via arguments. Other arguments are not, and should be provided via kwargs. Note boto3 expects these in
      CamelCase format, for example:

      .. code-block:: python

          list_training_jobs(name_contains="myjob", StatusEquals="Failed")

      .. seealso::
          https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_training_jobs

      :param name_contains: (optional) partial name to match
      :param max_results: (optional) maximum number of results to return. None returns infinite results
      :param kwargs: (optional) kwargs to boto3's list_training_jobs method
      :return: results of the list_training_jobs request



   
   .. method:: list_processing_jobs(self, **kwargs)

      This method wraps boto3's list_processing_jobs(). All arguments should be provided via kwargs.
      Note boto3 expects these in CamelCase format, for example:

      .. code-block:: python

          list_processing_jobs(NameContains="myjob", StatusEquals="Failed")

      .. seealso::
          https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_processing_jobs

      :param kwargs: (optional) kwargs to boto3's list_training_jobs method
      :return: results of the list_processing_jobs request



   
   .. method:: _list_request(self, partial_func: Callable, result_key: str, max_results: Optional[int] = None)

      All AWS boto3 list_* requests return results in batches (if the key "NextToken" is contained in the
      result, there are more results to fetch). The default AWS batch size is 10, and configurable up to
      100. This function iteratively loads all results (or up to a given maximum).

      Each boto3 list_* function returns the results in a list with a different name. The key of this
      structure must be given to iterate over the results, e.g. "TransformJobSummaries" for
      list_transform_jobs().

      :param partial_func: boto3 function with arguments
      :param result_key: the result key to iterate over
      :param max_results: maximum number of results to return (None = infinite)
      :return: Results of the list_* request




