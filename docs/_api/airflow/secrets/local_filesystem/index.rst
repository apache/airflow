:mod:`airflow.secrets.local_filesystem`
=======================================

.. py:module:: airflow.secrets.local_filesystem

.. autoapi-nested-parse::

   Objects relating to retrieving connections and variables from local file



Module Contents
---------------

.. data:: log
   

   

.. function:: get_connection_parameter_names() -> Set[str]
   Returns :class:`airflow.models.connection.Connection` constructor parameters.


.. function:: _parse_env_file(file_path: str) -> Tuple[Dict[str, List[str]], List[FileSyntaxError]]
   Parse a file in the ``.env`` format.

   .. code-block:: text

       MY_CONN_ID=my-conn-type://my-login:my-pa%2Fssword@my-host:5432/my-schema?param1=val1&param2=val2

   :param file_path: The location of the file that will be processed.
   :type file_path: str
   :return: Tuple with mapping of key and list of values and list of syntax errors


.. function:: _parse_yaml_file(file_path: str) -> Tuple[Dict[str, List[str]], List[FileSyntaxError]]
   Parse a file in the YAML format.

   :param file_path: The location of the file that will be processed.
   :type file_path: str
   :return: Tuple with mapping of key and list of values and list of syntax errors


.. function:: _parse_json_file(file_path: str) -> Tuple[Dict[str, Any], List[FileSyntaxError]]
   Parse a file in the JSON format.

   :param file_path: The location of the file that will be processed.
   :type file_path: str
   :return: Tuple with mapping of key and list of values and list of syntax errors


.. data:: FILE_PARSERS
   

   

.. function:: _parse_secret_file(file_path: str) -> Dict[str, Any]
   Based on the file extension format, selects a parser, and parses the file.

   :param file_path: The location of the file that will be processed.
   :type file_path: str
   :return: Map of secret key (e.g. connection ID) and value.


.. function:: _create_connection(conn_id: str, value: Any)
   Creates a connection based on a URL or JSON object.


.. function:: load_variables(file_path: str) -> Dict[str, str]
   Load variables from a text file.

   Both ``JSON`` and ``.env`` files are supported.

   :param file_path: The location of the file that will be processed.
   :type file_path: str
   :rtype: Dict[str, List[str]]


.. function:: load_connections(file_path) -> Dict[str, List[Any]]
   This function is deprecated. Please use `airflow.secrets.local_filesystem.load_connections_dict`.",


.. function:: load_connections_dict(file_path: str) -> Dict[str, Any]
   Load connection from text file.

   Both ``JSON`` and ``.env`` files are supported.

   :return: A dictionary where the key contains a connection ID and the value contains a list of connections.
   :rtype: Dict[str, airflow.models.connection.Connection]


.. py:class:: LocalFilesystemBackend(variables_file_path: Optional[str] = None, connections_file_path: Optional[str] = None)

   Bases: :class:`airflow.secrets.base_secrets.BaseSecretsBackend`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Retrieves Connection objects and Variables from local files

   Both ``JSON`` and ``.env`` files are supported.

   :param variables_file_path: File location with variables data.
   :type variables_file_path: str
   :param connections_file_path: File location with connection data.
   :type connections_file_path: str

   .. attribute:: _local_variables
      

      

   .. attribute:: _local_connections
      

      

   
   .. method:: get_connections(self, conn_id: str)



   
   .. method:: get_variable(self, key: str)




