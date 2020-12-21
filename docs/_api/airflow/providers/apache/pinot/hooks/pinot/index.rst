:mod:`airflow.providers.apache.pinot.hooks.pinot`
=================================================

.. py:module:: airflow.providers.apache.pinot.hooks.pinot


Module Contents
---------------

.. py:class:: PinotAdminHook(conn_id: str = 'pinot_admin_default', cmd_path: str = 'pinot-admin.sh', pinot_admin_system_exit: bool = False)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   This hook is a wrapper around the pinot-admin.sh script.
   For now, only small subset of its subcommands are implemented,
   which are required to ingest offline data into Apache Pinot
   (i.e., AddSchema, AddTable, CreateSegment, and UploadSegment).
   Their command options are based on Pinot v0.1.0.

   Unfortunately, as of v0.1.0, pinot-admin.sh always exits with
   status code 0. To address this behavior, users can use the
   pinot_admin_system_exit flag. If its value is set to false,
   this hook evaluates the result based on the output message
   instead of the status code. This Pinot's behavior is supposed
   to be improved in the next release, which will include the
   following PR: https://github.com/apache/incubator-pinot/pull/4110

   :param conn_id: The name of the connection to use.
   :type conn_id: str
   :param cmd_path: The filepath to the pinot-admin.sh executable
   :type cmd_path: str
   :param pinot_admin_system_exit: If true, the result is evaluated based on the status code.
                                   Otherwise, the result is evaluated as a failure if "Error" or
                                   "Exception" is in the output message.
   :type pinot_admin_system_exit: bool

   
   .. method:: get_conn(self)



   
   .. method:: add_schema(self, schema_file: str, with_exec: bool = True)

      Add Pinot schema by run AddSchema command

      :param schema_file: Pinot schema file
      :type schema_file: str
      :param with_exec: bool
      :type with_exec: bool



   
   .. method:: add_table(self, file_path: str, with_exec: bool = True)

      Add Pinot table with run AddTable command

      :param file_path: Pinot table configure file
      :type file_path: str
      :param with_exec: bool
      :type with_exec: bool



   
   .. method:: create_segment(self, generator_config_file: Optional[str] = None, data_dir: Optional[str] = None, segment_format: Optional[str] = None, out_dir: Optional[str] = None, overwrite: Optional[str] = None, table_name: Optional[str] = None, segment_name: Optional[str] = None, time_column_name: Optional[str] = None, schema_file: Optional[str] = None, reader_config_file: Optional[str] = None, enable_star_tree_index: Optional[str] = None, star_tree_index_spec_file: Optional[str] = None, hll_size: Optional[str] = None, hll_columns: Optional[str] = None, hll_suffix: Optional[str] = None, num_threads: Optional[str] = None, post_creation_verification: Optional[str] = None, retry: Optional[str] = None)

      Create Pinot segment by run CreateSegment command



   
   .. method:: upload_segment(self, segment_dir: str, table_name: Optional[str] = None)

      Upload Segment with run UploadSegment command

      :param segment_dir:
      :param table_name:
      :return:



   
   .. method:: run_cli(self, cmd: List[str], verbose: bool = True)

      Run command with pinot-admin.sh

      :param cmd: List of command going to be run by pinot-admin.sh script
      :type cmd: list
      :param verbose:
      :type verbose: bool




.. py:class:: PinotDbApiHook

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Interact with Pinot Broker Query API

   This hook uses standard-SQL endpoint since PQL endpoint is soon to be deprecated.
   https://docs.pinot.apache.org/users/api/querying-pinot-using-standard-sql

   .. attribute:: conn_name_attr
      :annotation: = pinot_broker_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = pinot_broker_default

      

   .. attribute:: supports_autocommit
      :annotation: = False

      

   
   .. method:: get_conn(self)

      Establish a connection to pinot broker through pinot dbapi.



   
   .. method:: get_uri(self)

      Get the connection uri for pinot broker.

      e.g: http://localhost:9000/query/sql



   
   .. method:: get_records(self, sql: str, parameters: Optional[Union[Dict[str, Any], Iterable[Any]]] = None)

      Executes the sql and returns a set of records.

      :param sql: the sql statement to be executed (str) or a list of
          sql statements to execute
      :type sql: str
      :param parameters: The parameters to render the SQL query with.
      :type parameters: dict or iterable



   
   .. method:: get_first(self, sql: str, parameters: Optional[Union[Dict[str, Any], Iterable[Any]]] = None)

      Executes the sql and returns the first resulting row.

      :param sql: the sql statement to be executed (str) or a list of
          sql statements to execute
      :type sql: str or list
      :param parameters: The parameters to render the SQL query with.
      :type parameters: dict or iterable



   
   .. method:: set_autocommit(self, conn: Connection, autocommit: Any)



   
   .. method:: insert_rows(self, table: str, rows: str, target_fields: Optional[str] = None, commit_every: int = 1000, replace: bool = False, **kwargs)




