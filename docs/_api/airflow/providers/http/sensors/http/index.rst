:mod:`airflow.providers.http.sensors.http`
==========================================

.. py:module:: airflow.providers.http.sensors.http


Module Contents
---------------

.. py:class:: HttpSensor(*, endpoint: str, http_conn_id: str = 'http_default', method: str = 'GET', request_params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, Any]] = None, response_check: Optional[Callable[..., bool]] = None, extra_options: Optional[Dict[str, Any]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Executes a HTTP GET statement and returns False on failure caused by
   404 Not Found or `response_check` returning False.

   HTTP Error codes other than 404 (like 403) or Connection Refused Error
   would fail the sensor itself directly (no more poking).

   The response check can access the template context to the operator:

       def response_check(response, task_instance):
           # The task_instance is injected, so you can pull data form xcom
           # Other context variables such as dag, ds, execution_date are also available.
           xcom_data = task_instance.xcom_pull(task_ids='pushing_task')
           # In practice you would do something more sensible with this data..
           print(xcom_data)
           return True

       HttpSensor(task_id='my_http_sensor', ..., response_check=response_check)

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:HttpSensor`

   :param http_conn_id: The connection to run the sensor against
   :type http_conn_id: str
   :param method: The HTTP request method to use
   :type method: str
   :param endpoint: The relative part of the full url
   :type endpoint: str
   :param request_params: The parameters to be added to the GET url
   :type request_params: a dictionary of string key/value pairs
   :param headers: The HTTP headers to be added to the GET request
   :type headers: a dictionary of string key/value pairs
   :param response_check: A check against the 'requests' response object.
       The callable takes the response object as the first positional argument
       and optionally any number of keyword arguments available in the context dictionary.
       It should return True for 'pass' and False otherwise.
   :type response_check: A lambda or defined function.
   :param extra_options: Extra options for the 'requests' library, see the
       'requests' documentation (options to modify timeout, ssl, etc.)
   :type extra_options: A dictionary of options, where key is string and value
       depends on the option that's being modified.

   .. attribute:: template_fields
      :annotation: = ['endpoint', 'request_params']

      

   
   .. method:: poke(self, context: Dict[Any, Any])




