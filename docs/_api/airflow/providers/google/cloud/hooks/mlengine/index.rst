:mod:`airflow.providers.google.cloud.hooks.mlengine`
====================================================

.. py:module:: airflow.providers.google.cloud.hooks.mlengine

.. autoapi-nested-parse::

   This module contains a Google ML Engine Hook.



Module Contents
---------------

.. data:: log
   

   

.. data:: _AIRFLOW_VERSION
   

   

.. function:: _poll_with_exponential_delay(request, execute_num_retries, max_n, is_done_func, is_error_func)
   Execute request with exponential delay.

   This method is intended to handle and retry in case of api-specific errors,
   such as 429 "Too Many Requests", unlike the `request.execute` which handles
   lower level errors like `ConnectionError`/`socket.timeout`/`ssl.SSLError`.

   :param request: request to be executed.
   :type request: googleapiclient.http.HttpRequest
   :param execute_num_retries: num_retries for `request.execute` method.
   :type execute_num_retries: int
   :param max_n: number of times to retry request in this method.
   :type max_n: int
   :param is_done_func: callable to determine if operation is done.
   :type is_done_func: callable
   :param is_error_func: callable to determine if operation is failed.
   :type is_error_func: callable
   :return: response
   :rtype: httplib2.Response


.. py:class:: MLEngineHook

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google ML Engine APIs.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   
   .. method:: get_conn(self)

      Retrieves the connection to MLEngine.

      :return: Google MLEngine services object.



   
   .. method:: create_job(self, job: dict, project_id: str, use_existing_job_fn: Optional[Callable] = None)

      Launches a MLEngine job and wait for it to reach a terminal state.

      :param project_id: The Google Cloud project id within which MLEngine
          job will be launched. If set to None or missing, the default project_id from the Google Cloud
          connection is used.
      :type project_id: str
      :param job: MLEngine Job object that should be provided to the MLEngine
          API, such as: ::

              {
                'jobId': 'my_job_id',
                'trainingInput': {
                  'scaleTier': 'STANDARD_1',
                  ...
                }
              }

      :type job: dict
      :param use_existing_job_fn: In case that a MLEngine job with the same
          job_id already exist, this method (if provided) will decide whether
          we should use this existing job, continue waiting for it to finish
          and returning the job object. It should accepts a MLEngine job
          object, and returns a boolean value indicating whether it is OK to
          reuse the existing job. If 'use_existing_job_fn' is not provided,
          we by default reuse the existing MLEngine job.
      :type use_existing_job_fn: function
      :return: The MLEngine job object if the job successfully reach a
          terminal state (which might be FAILED or CANCELLED state).
      :rtype: dict



   
   .. method:: cancel_job(self, job_id: str, project_id: str)

      Cancels a MLEngine job.

      :param project_id: The Google Cloud project id within which MLEngine
          job will be cancelled. If set to None or missing, the default project_id from the Google Cloud
          connection is used.
      :type project_id: str
      :param job_id: A unique id for the want-to-be cancelled Google MLEngine training job.
      :type job_id: str

      :return: Empty dict if cancelled successfully
      :rtype: dict
      :raises: googleapiclient.errors.HttpError



   
   .. method:: _get_job(self, project_id: str, job_id: str)

      Gets a MLEngine job based on the job id.

      :param project_id: The project in which the Job is located. If set to None or missing, the default
          project_id from the Google Cloud connection is used. (templated)
      :type project_id: str
      :param job_id: A unique id for the Google MLEngine job. (templated)
      :type job_id: str
      :return: MLEngine job object if succeed.
      :rtype: dict
      :raises: googleapiclient.errors.HttpError



   
   .. method:: _wait_for_job_done(self, project_id: str, job_id: str, interval: int = 30)

      Waits for the Job to reach a terminal state.

      This method will periodically check the job state until the job reach
      a terminal state.

      :param project_id: The project in which the Job is located. If set to None or missing, the default
          project_id from the Google Cloud connection is used. (templated)
      :type project_id: str
      :param job_id: A unique id for the Google MLEngine job. (templated)
      :type job_id: str
      :param interval: Time expressed in seconds after which the job status is checked again. (templated)
      :type interval: int
      :raises: googleapiclient.errors.HttpError



   
   .. method:: create_version(self, model_name: str, version_spec: Dict, project_id: str)

      Creates the Version on Google Cloud ML Engine.

      :param version_spec: A dictionary containing the information about the version. (templated)
      :type version_spec: dict
      :param model_name: The name of the Google Cloud ML Engine model that the version belongs to.
          (templated)
      :type model_name: str
      :param project_id: The Google Cloud project name to which MLEngine model belongs.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
          (templated)
      :type project_id: str
      :return: If the version was created successfully, returns the operation.
          Otherwise raises an error .
      :rtype: dict



   
   .. method:: set_default_version(self, model_name: str, version_name: str, project_id: str)

      Sets a version to be the default. Blocks until finished.

      :param model_name: The name of the Google Cloud ML Engine model that the version belongs to.
          (templated)
      :type model_name: str
      :param version_name: A name to use for the version being operated upon. (templated)
      :type version_name: str
      :param project_id: The Google Cloud project name to which MLEngine model belongs. If set to None
          or missing, the default project_id from the Google Cloud connection is used. (templated)
      :type project_id: str
      :return: If successful, return an instance of Version.
          Otherwise raises an error.
      :rtype: dict
      :raises: googleapiclient.errors.HttpError



   
   .. method:: list_versions(self, model_name: str, project_id: str)

      Lists all available versions of a model. Blocks until finished.

      :param model_name: The name of the Google Cloud ML Engine model that the version
          belongs to. (templated)
      :type model_name: str
      :param project_id: The Google Cloud project name to which MLEngine model belongs. If set to None or
          missing, the default project_id from the Google Cloud connection is used. (templated)
      :type project_id: str
      :return: return an list of instance of Version.
      :rtype: List[Dict]
      :raises: googleapiclient.errors.HttpError



   
   .. method:: delete_version(self, model_name: str, version_name: str, project_id: str)

      Deletes the given version of a model. Blocks until finished.

      :param model_name: The name of the Google Cloud ML Engine model that the version
          belongs to. (templated)
      :type model_name: str
      :param project_id: The Google Cloud project name to which MLEngine
          model belongs.
      :type project_id: str
      :return: If the version was deleted successfully, returns the operation.
          Otherwise raises an error.
      :rtype: Dict



   
   .. method:: create_model(self, model: dict, project_id: str)

      Create a Model. Blocks until finished.

      :param model: A dictionary containing the information about the model.
      :type model: dict
      :param project_id: The Google Cloud project name to which MLEngine model belongs. If set to None or
          missing, the default project_id from the Google Cloud connection is used. (templated)
      :type project_id: str
      :return: If the version was created successfully, returns the instance of Model.
          Otherwise raises an error.
      :rtype: Dict
      :raises: googleapiclient.errors.HttpError



   
   .. method:: get_model(self, model_name: str, project_id: str)

      Gets a Model. Blocks until finished.

      :param model_name: The name of the model.
      :type model_name: str
      :param project_id: The Google Cloud project name to which MLEngine model belongs. If set to None
          or missing, the default project_id from the Google Cloud connection is used. (templated)
      :type project_id: str
      :return: If the model exists, returns the instance of Model.
          Otherwise return None.
      :rtype: Dict
      :raises: googleapiclient.errors.HttpError



   
   .. method:: delete_model(self, model_name: str, project_id: str, delete_contents: bool = False)

      Delete a Model. Blocks until finished.

      :param model_name: The name of the model.
      :type model_name: str
      :param delete_contents: Whether to force the deletion even if the models is not empty.
          Will delete all version (if any) in the dataset if set to True.
          The default value is False.
      :type delete_contents: bool
      :param project_id: The Google Cloud project name to which MLEngine model belongs. If set to None
          or missing, the default project_id from the Google Cloud connection is used. (templated)
      :type project_id: str
      :raises: googleapiclient.errors.HttpError



   
   .. method:: _delete_all_versions(self, model_name: str, project_id: str)



   
   .. method:: _append_label(self, model: dict)




