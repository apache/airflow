:mod:`airflow.providers.google.cloud.hooks.dataflow`
====================================================

.. py:module:: airflow.providers.google.cloud.hooks.dataflow

.. autoapi-nested-parse::

   This module contains a Google Dataflow Hook.



Module Contents
---------------

.. data:: DEFAULT_DATAFLOW_LOCATION
   :annotation: = us-central1

   

.. data:: JOB_ID_PATTERN
   

   

.. data:: T
   

   

.. function:: _fallback_variable_parameter(parameter_name: str, variable_key_name: str) -> Callable[[T], T]

.. data:: _fallback_to_location_from_variables
   

   

.. data:: _fallback_to_project_id_from_variables
   

   

.. py:class:: DataflowJobStatus

   Helper class with Dataflow job statuses.
   Reference: https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs#Job.JobState

   .. attribute:: JOB_STATE_DONE
      :annotation: = JOB_STATE_DONE

      

   .. attribute:: JOB_STATE_UNKNOWN
      :annotation: = JOB_STATE_UNKNOWN

      

   .. attribute:: JOB_STATE_STOPPED
      :annotation: = JOB_STATE_STOPPED

      

   .. attribute:: JOB_STATE_RUNNING
      :annotation: = JOB_STATE_RUNNING

      

   .. attribute:: JOB_STATE_FAILED
      :annotation: = JOB_STATE_FAILED

      

   .. attribute:: JOB_STATE_CANCELLED
      :annotation: = JOB_STATE_CANCELLED

      

   .. attribute:: JOB_STATE_UPDATED
      :annotation: = JOB_STATE_UPDATED

      

   .. attribute:: JOB_STATE_DRAINING
      :annotation: = JOB_STATE_DRAINING

      

   .. attribute:: JOB_STATE_DRAINED
      :annotation: = JOB_STATE_DRAINED

      

   .. attribute:: JOB_STATE_PENDING
      :annotation: = JOB_STATE_PENDING

      

   .. attribute:: JOB_STATE_CANCELLING
      :annotation: = JOB_STATE_CANCELLING

      

   .. attribute:: JOB_STATE_QUEUED
      :annotation: = JOB_STATE_QUEUED

      

   .. attribute:: FAILED_END_STATES
      

      

   .. attribute:: SUCCEEDED_END_STATES
      

      

   .. attribute:: TERMINAL_STATES
      

      

   .. attribute:: AWAITING_STATES
      

      


.. py:class:: DataflowJobType

   Helper class with Dataflow job types.

   .. attribute:: JOB_TYPE_UNKNOWN
      :annotation: = JOB_TYPE_UNKNOWN

      

   .. attribute:: JOB_TYPE_BATCH
      :annotation: = JOB_TYPE_BATCH

      

   .. attribute:: JOB_TYPE_STREAMING
      :annotation: = JOB_TYPE_STREAMING

      


.. py:class:: _DataflowJobsController(dataflow: Any, project_number: str, location: str, poll_sleep: int = 10, name: Optional[str] = None, job_id: Optional[str] = None, num_retries: int = 0, multiple_jobs: bool = False, drain_pipeline: bool = False, cancel_timeout: Optional[int] = 5 * 60, wait_until_finished: Optional[bool] = None)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Interface for communication with Google API.

   It's not use Apache Beam, but only Google Dataflow API.

   :param dataflow: Discovery resource
   :param project_number: The Google Cloud Project ID.
   :param location: Job location.
   :param poll_sleep: The status refresh rate for pending operations.
   :param name: The Job ID prefix used when the multiple_jobs option is passed is set to True.
   :param job_id: ID of a single job.
   :param num_retries: Maximum number of retries in case of connection problems.
   :param multiple_jobs: If set to true this task will be searched by name prefix (``name`` parameter),
       not by specific job ID, then actions will be performed on all matching jobs.
   :param drain_pipeline: Optional, set to True if want to stop streaming job by draining it
       instead of canceling.
   :param cancel_timeout: wait time in seconds for successful job canceling
   :param wait_until_finished: If True, wait for the end of pipeline execution before exiting. If False,
       it only submits job and check once is job not in terminal state.

       The default behavior depends on the type of pipeline:

       * for the streaming pipeline, wait for jobs to start,
       * for the batch pipeline, wait for the jobs to complete.

   
   .. method:: is_job_running(self)

      Helper method to check if jos is still running in dataflow

      :return: True if job is running.
      :rtype: bool



   
   .. method:: _get_current_jobs(self)

      Helper method to get list of jobs that start with job name or id

      :return: list of jobs including id's
      :rtype: list



   
   .. method:: fetch_job_by_id(self, job_id: str)

      Helper method to fetch the job with the specified Job ID.

      :param job_id: Job ID to get.
      :type job_id: str
      :return: the Job
      :rtype: dict



   
   .. method:: fetch_job_metrics_by_id(self, job_id: str)

      Helper method to fetch the job metrics with the specified Job ID.

      :param job_id: Job ID to get.
      :type job_id: str
      :return: the JobMetrics. See:
          https://cloud.google.com/dataflow/docs/reference/rest/v1b3/JobMetrics
      :rtype: dict



   
   .. method:: _fetch_all_jobs(self)



   
   .. method:: _fetch_jobs_by_prefix_name(self, prefix_name: str)



   
   .. method:: _refresh_jobs(self)

      Helper method to get all jobs by name

      :return: jobs
      :rtype: list



   
   .. method:: _check_dataflow_job_state(self, job)

      Helper method to check the state of one job in dataflow for this task
      if job failed raise exception

      :return: True if job is done.
      :rtype: bool
      :raise: Exception



   
   .. method:: wait_for_done(self)

      Helper method to wait for result of submitted job.



   
   .. method:: get_jobs(self, refresh: bool = False)

      Returns Dataflow jobs.

      :param refresh: Forces the latest data to be fetched.
      :type refresh: bool
      :return: list of jobs
      :rtype: list



   
   .. method:: _wait_for_states(self, expected_states: Set[str])

      Waiting for the jobs to reach a certain state.



   
   .. method:: cancel(self)

      Cancels or drains current job




.. py:class:: _DataflowRunner(cmd: List[str], on_new_job_id_callback: Optional[Callable[[str], None]] = None)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   
   .. method:: _process_fd(self, fd)

      Prints output to logs and lookup for job ID in each line.

      :param fd: File descriptor.



   
   .. method:: _process_line_and_extract_job_id(self, line: str)

      Extracts job_id.

      :param line: URL from which job_id has to be extracted
      :type line: str



   
   .. method:: wait_for_done(self)

      Waits for Dataflow job to complete.

      :return: Job id
      :rtype: Optional[str]




.. py:class:: DataflowHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, poll_sleep: int = 10, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, drain_pipeline: bool = False, cancel_timeout: Optional[int] = 5 * 60, wait_until_finished: Optional[bool] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Dataflow.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   
   .. method:: get_conn(self)

      Returns a Google Cloud Dataflow service object.



   
   .. method:: _start_dataflow(self, variables: dict, name: str, command_prefix: List[str], project_id: str, multiple_jobs: bool = False, on_new_job_id_callback: Optional[Callable[[str], None]] = None, location: str = DEFAULT_DATAFLOW_LOCATION)



   
   .. method:: start_java_dataflow(self, job_name: str, variables: dict, jar: str, project_id: str, job_class: Optional[str] = None, append_job_name: bool = True, multiple_jobs: bool = False, on_new_job_id_callback: Optional[Callable[[str], None]] = None, location: str = DEFAULT_DATAFLOW_LOCATION)

      Starts Dataflow java job.

      :param job_name: The name of the job.
      :type job_name: str
      :param variables: Variables passed to the job.
      :type variables: dict
      :param project_id: Optional, the Google Cloud project ID in which to start a job.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
      :param jar: Name of the jar for the job
      :type job_class: str
      :param job_class: Name of the java class for the job.
      :type job_class: str
      :param append_job_name: True if unique suffix has to be appended to job name.
      :type append_job_name: bool
      :param multiple_jobs: True if to check for multiple job in dataflow
      :type multiple_jobs: bool
      :param on_new_job_id_callback: Callback called when the job ID is known.
      :type on_new_job_id_callback: callable
      :param location: Job location.
      :type location: str



   
   .. method:: start_template_dataflow(self, job_name: str, variables: dict, parameters: dict, dataflow_template: str, project_id: str, append_job_name: bool = True, on_new_job_id_callback: Optional[Callable[[str], None]] = None, location: str = DEFAULT_DATAFLOW_LOCATION, environment: Optional[dict] = None)

      Starts Dataflow template job.

      :param job_name: The name of the job.
      :type job_name: str
      :param variables: Map of job runtime environment options.
          It will update environment argument if passed.

          .. seealso::
              For more information on possible configurations, look at the API documentation
              `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
              <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__

      :type variables: dict
      :param parameters: Parameters fot the template
      :type parameters: dict
      :param dataflow_template: GCS path to the template.
      :type dataflow_template: str
      :param project_id: Optional, the Google Cloud project ID in which to start a job.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
      :param append_job_name: True if unique suffix has to be appended to job name.
      :type append_job_name: bool
      :param on_new_job_id_callback: Callback called when the job ID is known.
      :type on_new_job_id_callback: callable
      :param location: Job location.
      :type location: str
      :type environment: Optional, Map of job runtime environment options.

          .. seealso::
              For more information on possible configurations, look at the API documentation
              `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
              <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__

      :type environment: Optional[dict]



   
   .. method:: start_flex_template(self, body: dict, location: str, project_id: str, on_new_job_id_callback: Optional[Callable[[str], None]] = None)

      Starts flex templates with the Dataflow  pipeline.

      :param body: The request body. See:
          https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.flexTemplates/launch#request-body
      :param location: The location of the Dataflow job (for example europe-west1)
      :type location: str
      :param project_id: The ID of the GCP project that owns the job.
          If set to ``None`` or missing, the default project_id from the GCP connection is used.
      :type project_id: Optional[str]
      :param on_new_job_id_callback: A callback that is called when a Job ID is detected.
      :return: the Job



   
   .. method:: start_python_dataflow(self, job_name: str, variables: dict, dataflow: str, py_options: List[str], project_id: str, py_interpreter: str = 'python3', py_requirements: Optional[List[str]] = None, py_system_site_packages: bool = False, append_job_name: bool = True, on_new_job_id_callback: Optional[Callable[[str], None]] = None, location: str = DEFAULT_DATAFLOW_LOCATION)

      Starts Dataflow job.

      :param job_name: The name of the job.
      :type job_name: str
      :param variables: Variables passed to the job.
      :type variables: Dict
      :param dataflow: Name of the Dataflow process.
      :type dataflow: str
      :param py_options: Additional options.
      :type py_options: List[str]
      :param project_id: The ID of the GCP project that owns the job.
          If set to ``None`` or missing, the default project_id from the GCP connection is used.
      :type project_id: Optional[str]
      :param py_interpreter: Python version of the beam pipeline.
          If None, this defaults to the python3.
          To track python versions supported by beam and related
          issues check: https://issues.apache.org/jira/browse/BEAM-1251
      :param py_requirements: Additional python package(s) to install.
          If a value is passed to this parameter, a new virtual environment has been created with
          additional packages installed.

          You could also install the apache-beam package if it is not installed on your system or you want
          to use a different version.
      :type py_requirements: List[str]
      :param py_system_site_packages: Whether to include system_site_packages in your virtualenv.
          See virtualenv documentation for more information.

          This option is only relevant if the ``py_requirements`` parameter is not None.
      :type py_interpreter: str
      :param append_job_name: True if unique suffix has to be appended to job name.
      :type append_job_name: bool
      :param project_id: Optional, the Google Cloud project ID in which to start a job.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
      :param on_new_job_id_callback: Callback called when the job ID is known.
      :type on_new_job_id_callback: callable
      :param location: Job location.
      :type location: str



   
   .. staticmethod:: _build_dataflow_job_name(job_name: str, append_job_name: bool = True)



   
   .. staticmethod:: _options_to_args(variables: dict)



   
   .. method:: is_job_dataflow_running(self, name: str, project_id: str, location: str = DEFAULT_DATAFLOW_LOCATION, variables: Optional[dict] = None)

      Helper method to check if jos is still running in dataflow

      :param name: The name of the job.
      :type name: str
      :param project_id: Optional, the Google Cloud project ID in which to start a job.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :param location: Job location.
      :type location: str
      :return: True if job is running.
      :rtype: bool



   
   .. method:: cancel_job(self, project_id: str, job_name: Optional[str] = None, job_id: Optional[str] = None, location: str = DEFAULT_DATAFLOW_LOCATION)

      Cancels the job with the specified name prefix or Job ID.

      Parameter ``name`` and ``job_id`` are mutually exclusive.

      :param job_name: Name prefix specifying which jobs are to be canceled.
      :type job_name: str
      :param job_id: Job ID specifying which jobs are to be canceled.
      :type job_id: str
      :param location: Job location.
      :type location: str
      :param project_id: Optional, the Google Cloud project ID in which to start a job.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id:



   
   .. method:: start_sql_job(self, job_name: str, query: str, options: Dict[str, Any], project_id: str, location: str = DEFAULT_DATAFLOW_LOCATION, on_new_job_id_callback: Optional[Callable[[str], None]] = None)

      Starts Dataflow SQL query.

      :param job_name: The unique name to assign to the Cloud Dataflow job.
      :type job_name: str
      :param query: The SQL query to execute.
      :type query: str
      :param options: Job parameters to be executed.
          For more information, look at:
          `https://cloud.google.com/sdk/gcloud/reference/beta/dataflow/sql/query
          <gcloud beta dataflow sql query>`__
          command reference
      :param location: The location of the Dataflow job (for example europe-west1)
      :type location: str
      :param project_id: The ID of the GCP project that owns the job.
          If set to ``None`` or missing, the default project_id from the GCP connection is used.
      :type project_id: Optional[str]
      :param on_new_job_id_callback: Callback called when the job ID is known.
      :type on_new_job_id_callback: callable
      :return: the new job object



   
   .. method:: get_job(self, job_id: str, project_id: str, location: str = DEFAULT_DATAFLOW_LOCATION)

      Gets the job with the specified Job ID.

      :param job_id: Job ID to get.
      :type job_id: str
      :param project_id: Optional, the Google Cloud project ID in which to start a job.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id:
      :param location: The location of the Dataflow job (for example europe-west1). See:
          https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
      :return: the Job
      :rtype: dict



   
   .. method:: fetch_job_metrics_by_id(self, job_id: str, project_id: str, location: str = DEFAULT_DATAFLOW_LOCATION)

      Gets the job metrics with the specified Job ID.

      :param job_id: Job ID to get.
      :type job_id: str
      :param project_id: Optional, the Google Cloud project ID in which to start a job.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id:
      :param location: The location of the Dataflow job (for example europe-west1). See:
          https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
      :return: the JobMetrics. See:
          https://cloud.google.com/dataflow/docs/reference/rest/v1b3/JobMetrics
      :rtype: dict




