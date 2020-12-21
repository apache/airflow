:mod:`airflow.providers.databricks.hooks.databricks`
====================================================

.. py:module:: airflow.providers.databricks.hooks.databricks

.. autoapi-nested-parse::

   Databricks hook.

   This hook enable the submitting and running of jobs to the Databricks platform. Internally the
   operators talk to the ``api/2.0/jobs/runs/submit``
   `endpoint <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_.



Module Contents
---------------

.. data:: RESTART_CLUSTER_ENDPOINT
   :annotation: = ['POST', 'api/2.0/clusters/restart']

   

.. data:: START_CLUSTER_ENDPOINT
   :annotation: = ['POST', 'api/2.0/clusters/start']

   

.. data:: TERMINATE_CLUSTER_ENDPOINT
   :annotation: = ['POST', 'api/2.0/clusters/delete']

   

.. data:: RUN_NOW_ENDPOINT
   :annotation: = ['POST', 'api/2.0/jobs/run-now']

   

.. data:: SUBMIT_RUN_ENDPOINT
   :annotation: = ['POST', 'api/2.0/jobs/runs/submit']

   

.. data:: GET_RUN_ENDPOINT
   :annotation: = ['GET', 'api/2.0/jobs/runs/get']

   

.. data:: CANCEL_RUN_ENDPOINT
   :annotation: = ['POST', 'api/2.0/jobs/runs/cancel']

   

.. data:: USER_AGENT_HEADER
   

   

.. data:: INSTALL_LIBS_ENDPOINT
   :annotation: = ['POST', 'api/2.0/libraries/install']

   

.. data:: UNINSTALL_LIBS_ENDPOINT
   :annotation: = ['POST', 'api/2.0/libraries/uninstall']

   

.. py:class:: RunState(life_cycle_state: str, result_state: str, state_message: str)

   Utility class for the run state concept of Databricks runs.

   .. attribute:: is_terminal
      

      True if the current state is a terminal state.


   .. attribute:: is_successful
      

      True if the result state is SUCCESS


   
   .. method:: __eq__(self, other: object)



   
   .. method:: __repr__(self)




.. py:class:: DatabricksHook(databricks_conn_id: str = 'databricks_default', timeout_seconds: int = 180, retry_limit: int = 3, retry_delay: float = 1.0)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Interact with Databricks.

   :param databricks_conn_id: The name of the databricks connection to use.
   :type databricks_conn_id: str
   :param timeout_seconds: The amount of time in seconds the requests library
       will wait before timing-out.
   :type timeout_seconds: int
   :param retry_limit: The number of times to retry the connection in case of
       service outages.
   :type retry_limit: int
   :param retry_delay: The number of seconds to wait between retries (it
       might be a floating point number).
   :type retry_delay: float

   
   .. staticmethod:: _parse_host(host: str)

      The purpose of this function is to be robust to improper connections
      settings provided by users, specifically in the host field.

      For example -- when users supply ``https://xx.cloud.databricks.com`` as the
      host, we must strip out the protocol to get the host.::

          h = DatabricksHook()
          assert h._parse_host('https://xx.cloud.databricks.com') ==                 'xx.cloud.databricks.com'

      In the case where users supply the correct ``xx.cloud.databricks.com`` as the
      host, this function is a no-op.::

          assert h._parse_host('xx.cloud.databricks.com') == 'xx.cloud.databricks.com'



   
   .. method:: _do_api_call(self, endpoint_info, json)

      Utility function to perform an API call with retries

      :param endpoint_info: Tuple of method and endpoint
      :type endpoint_info: tuple[string, string]
      :param json: Parameters for this API call.
      :type json: dict
      :return: If the api call returns a OK status code,
          this function returns the response in JSON. Otherwise,
          we throw an AirflowException.
      :rtype: dict



   
   .. method:: _log_request_error(self, attempt_num: int, error: str)



   
   .. method:: run_now(self, json: dict)

      Utility function to call the ``api/2.0/jobs/run-now`` endpoint.

      :param json: The data used in the body of the request to the ``run-now`` endpoint.
      :type json: dict
      :return: the run_id as a string
      :rtype: str



   
   .. method:: submit_run(self, json: dict)

      Utility function to call the ``api/2.0/jobs/runs/submit`` endpoint.

      :param json: The data used in the body of the request to the ``submit`` endpoint.
      :type json: dict
      :return: the run_id as a string
      :rtype: str



   
   .. method:: get_run_page_url(self, run_id: str)

      Retrieves run_page_url.

      :param run_id: id of the run
      :return: URL of the run page



   
   .. method:: get_job_id(self, run_id: str)

      Retrieves job_id from run_id.

      :param run_id: id of the run
      :type run_id: str
      :return: Job id for given Databricks run



   
   .. method:: get_run_state(self, run_id: str)

      Retrieves run state of the run.

      :param run_id: id of the run
      :return: state of the run



   
   .. method:: cancel_run(self, run_id: str)

      Cancels the run.

      :param run_id: id of the run



   
   .. method:: restart_cluster(self, json: dict)

      Restarts the cluster.

      :param json: json dictionary containing cluster specification.



   
   .. method:: start_cluster(self, json: dict)

      Starts the cluster.

      :param json: json dictionary containing cluster specification.



   
   .. method:: terminate_cluster(self, json: dict)

      Terminates the cluster.

      :param json: json dictionary containing cluster specification.



   
   .. method:: install(self, json: dict)

      Install libraries on the cluster.

      Utility function to call the ``2.0/libraries/install`` endpoint.

      :param json: json dictionary containing cluster_id and an array of library
      :type json: dict



   
   .. method:: uninstall(self, json: dict)

      Uninstall libraries on the cluster.

      Utility function to call the ``2.0/libraries/uninstall`` endpoint.

      :param json: json dictionary containing cluster_id and an array of library
      :type json: dict




.. function:: _retryable_error(exception) -> bool

.. data:: RUN_LIFE_CYCLE_STATES
   :annotation: = ['PENDING', 'RUNNING', 'TERMINATING', 'TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']

   

.. py:class:: _TokenAuth(token: str)

   Bases: :class:`requests.auth.AuthBase`

   Helper class for requests Auth field. AuthBase requires you to implement the __call__
   magic function.

   
   .. method:: __call__(self, r: PreparedRequest)




