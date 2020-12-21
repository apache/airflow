:mod:`airflow.providers.http.operators.http`
============================================

.. py:module:: airflow.providers.http.operators.http


Module Contents
---------------

.. py:class:: SimpleHttpOperator(*, endpoint: Optional[str] = None, method: str = 'POST', data: Any = None, headers: Optional[Dict[str, str]] = None, response_check: Optional[Callable[..., bool]] = None, response_filter: Optional[Callable[..., Any]] = None, extra_options: Optional[Dict[str, Any]] = None, http_conn_id: str = 'http_default', log_response: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Calls an endpoint on an HTTP system to execute an action

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SimpleHttpOperator`

   :param http_conn_id: The connection to run the operator against
   :type http_conn_id: str
   :param endpoint: The relative part of the full url. (templated)
   :type endpoint: str
   :param method: The HTTP method to use, default = "POST"
   :type method: str
   :param data: The data to pass. POST-data in POST/PUT and params
       in the URL for a GET request. (templated)
   :type data: For POST/PUT, depends on the content-type parameter,
       for GET a dictionary of key/value string pairs
   :param headers: The HTTP headers to be added to the GET request
   :type headers: a dictionary of string key/value pairs
   :param response_check: A check against the 'requests' response object.
       The callable takes the response object as the first positional argument
       and optionally any number of keyword arguments available in the context dictionary.
       It should return True for 'pass' and False otherwise.
   :type response_check: A lambda or defined function.
   :param response_filter: A function allowing you to manipulate the response
       text. e.g response_filter=lambda response: json.loads(response.text).
       The callable takes the response object as the first positional argument
       and optionally any number of keyword arguments available in the context dictionary.
   :type response_filter: A lambda or defined function.
   :param extra_options: Extra options for the 'requests' library, see the
       'requests' documentation (options to modify timeout, ssl, etc.)
   :type extra_options: A dictionary of options, where key is string and value
       depends on the option that's being modified.
   :param log_response: Log the response (default: False)
   :type log_response: bool

   .. attribute:: template_fields
      :annotation: = ['endpoint', 'data', 'headers']

      

   .. attribute:: template_fields_renderers
      

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #f4a460

      

   
   .. method:: execute(self, context: Dict[str, Any])




