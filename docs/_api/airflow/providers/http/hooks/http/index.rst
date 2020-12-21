:mod:`airflow.providers.http.hooks.http`
========================================

.. py:module:: airflow.providers.http.hooks.http


Module Contents
---------------

.. py:class:: HttpHook(method: str = 'POST', http_conn_id: str = 'http_default', auth_type: Any = HTTPBasicAuth)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Interact with HTTP servers.

   :param method: the API method to be called
   :type method: str
   :param http_conn_id: connection that has the base API url i.e https://www.google.com/
       and optional authentication credentials. Default headers can also be specified in
       the Extra field in json format.
   :type http_conn_id: str
   :param auth_type: The auth type for the service
   :type auth_type: AuthBase of python requests lib

   
   .. method:: get_conn(self, headers: Optional[Dict[Any, Any]] = None)

      Returns http session for use with requests

      :param headers: additional headers to be passed through as a dictionary
      :type headers: dict



   
   .. method:: run(self, endpoint: Optional[str], data: Optional[Union[Dict[str, Any], str]] = None, headers: Optional[Dict[str, Any]] = None, extra_options: Optional[Dict[str, Any]] = None, **request_kwargs)

      Performs the request

      :param endpoint: the endpoint to be called i.e. resource/v1/query?
      :type endpoint: str
      :param data: payload to be uploaded or request parameters
      :type data: dict
      :param headers: additional headers to be passed through as a dictionary
      :type headers: dict
      :param extra_options: additional options to be used when executing the request
          i.e. {'check_response': False} to avoid checking raising exceptions on non
          2XX or 3XX status codes
      :type extra_options: dict
      :param  \**request_kwargs: Additional kwargs to pass when creating a request.
          For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``



   
   .. method:: check_response(self, response: requests.Response)

      Checks the status code and raise an AirflowException exception on non 2XX or 3XX
      status codes

      :param response: A requests response object
      :type response: requests.response



   
   .. method:: run_and_check(self, session: requests.Session, prepped_request: requests.PreparedRequest, extra_options: Dict[Any, Any])

      Grabs extra options like timeout and actually runs the request,
      checking for the result

      :param session: the session to be used to execute the request
      :type session: requests.Session
      :param prepped_request: the prepared request generated in run()
      :type prepped_request: session.prepare_request
      :param extra_options: additional options to be used when executing the request
          i.e. {'check_response': False} to avoid checking raising exceptions on non 2XX
          or 3XX status codes
      :type extra_options: dict



   
   .. method:: run_with_advanced_retry(self, _retry_args: Dict[Any, Any], *args, **kwargs)

      Runs Hook.run() with a Tenacity decorator attached to it. This is useful for
      connectors which might be disturbed by intermittent issues and should not
      instantly fail.

      :param _retry_args: Arguments which define the retry behaviour.
          See Tenacity documentation at https://github.com/jd/tenacity
      :type _retry_args: dict


      .. code-block:: python

          hook = HttpHook(http_conn_id='my_conn',method='GET')
          retry_args = dict(
               wait=tenacity.wait_exponential(),
               stop=tenacity.stop_after_attempt(10),
               retry=requests.exceptions.ConnectionError
           )
           hook.run_with_advanced_retry(
                   endpoint='v1/test',
                   _retry_args=retry_args
               )




