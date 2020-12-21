:mod:`airflow.providers.apache.sqoop.hooks.sqoop`
=================================================

.. py:module:: airflow.providers.apache.sqoop.hooks.sqoop

.. autoapi-nested-parse::

   This module contains a sqoop 1.x hook



Module Contents
---------------

.. py:class:: SqoopHook(conn_id: str = 'sqoop_default', verbose: bool = False, num_mappers: Optional[int] = None, hcatalog_database: Optional[str] = None, hcatalog_table: Optional[str] = None, properties: Optional[Dict[str, Any]] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   This hook is a wrapper around the sqoop 1 binary. To be able to use the hook
   it is required that "sqoop" is in the PATH.

   Additional arguments that can be passed via the 'extra' JSON field of the
   sqoop connection:

       * ``job_tracker``: Job tracker local|jobtracker:port.
       * ``namenode``: Namenode.
       * ``lib_jars``: Comma separated jar files to include in the classpath.
       * ``files``: Comma separated files to be copied to the map reduce cluster.
       * ``archives``: Comma separated archives to be unarchived on the compute
           machines.
       * ``password_file``: Path to file containing the password.

   :param conn_id: Reference to the sqoop connection.
   :type conn_id: str
   :param verbose: Set sqoop to verbose.
   :type verbose: bool
   :param num_mappers: Number of map tasks to import in parallel.
   :type num_mappers: int
   :param properties: Properties to set via the -D argument
   :type properties: dict

   
   .. method:: get_conn(self)



   
   .. method:: cmd_mask_password(self, cmd_orig: List[str])

      Mask command password for safety



   
   .. method:: popen(self, cmd: List[str], **kwargs)

      Remote Popen

      :param cmd: command to remotely execute
      :param kwargs: extra arguments to Popen (see subprocess.Popen)
      :return: handle to subprocess



   
   .. method:: _prepare_command(self, export: bool = False)



   
   .. staticmethod:: _get_export_format_argument(file_type: str = 'text')



   
   .. method:: _import_cmd(self, target_dir: Optional[str], append: bool, file_type: str, split_by: Optional[str], direct: Optional[bool], driver: Any, extra_import_options: Any)



   
   .. method:: import_table(self, table: str, target_dir: Optional[str] = None, append: bool = False, file_type: str = 'text', columns: Optional[str] = None, split_by: Optional[str] = None, where: Optional[str] = None, direct: bool = False, driver: Any = None, extra_import_options: Optional[Dict[str, Any]] = None)

      Imports table from remote location to target dir. Arguments are
      copies of direct sqoop command line arguments

      :param table: Table to read
      :param target_dir: HDFS destination dir
      :param append: Append data to an existing dataset in HDFS
      :param file_type: "avro", "sequence", "text" or "parquet".
          Imports data to into the specified format. Defaults to text.
      :param columns: <col,col,col…> Columns to import from table
      :param split_by: Column of the table used to split work units
      :param where: WHERE clause to use during import
      :param direct: Use direct connector if exists for the database
      :param driver: Manually specify JDBC driver class to use
      :param extra_import_options: Extra import options to pass as dict.
          If a key doesn't have a value, just pass an empty string to it.
          Don't include prefix of -- for sqoop options.



   
   .. method:: import_query(self, query: str, target_dir: Optional[str] = None, append: bool = False, file_type: str = 'text', split_by: Optional[str] = None, direct: Optional[bool] = None, driver: Optional[Any] = None, extra_import_options: Optional[Dict[str, Any]] = None)

      Imports a specific query from the rdbms to hdfs

      :param query: Free format query to run
      :param target_dir: HDFS destination dir
      :param append: Append data to an existing dataset in HDFS
      :param file_type: "avro", "sequence", "text" or "parquet"
          Imports data to hdfs into the specified format. Defaults to text.
      :param split_by: Column of the table used to split work units
      :param direct: Use direct import fast path
      :param driver: Manually specify JDBC driver class to use
      :param extra_import_options: Extra import options to pass as dict.
          If a key doesn't have a value, just pass an empty string to it.
          Don't include prefix of -- for sqoop options.



   
   .. method:: _export_cmd(self, table: str, export_dir: Optional[str] = None, input_null_string: Optional[str] = None, input_null_non_string: Optional[str] = None, staging_table: Optional[str] = None, clear_staging_table: bool = False, enclosed_by: Optional[str] = None, escaped_by: Optional[str] = None, input_fields_terminated_by: Optional[str] = None, input_lines_terminated_by: Optional[str] = None, input_optionally_enclosed_by: Optional[str] = None, batch: bool = False, relaxed_isolation: bool = False, extra_export_options: Optional[Dict[str, Any]] = None)



   
   .. method:: export_table(self, table: str, export_dir: Optional[str] = None, input_null_string: Optional[str] = None, input_null_non_string: Optional[str] = None, staging_table: Optional[str] = None, clear_staging_table: bool = False, enclosed_by: Optional[str] = None, escaped_by: Optional[str] = None, input_fields_terminated_by: Optional[str] = None, input_lines_terminated_by: Optional[str] = None, input_optionally_enclosed_by: Optional[str] = None, batch: bool = False, relaxed_isolation: bool = False, extra_export_options: Optional[Dict[str, Any]] = None)

      Exports Hive table to remote location. Arguments are copies of direct
      sqoop command line Arguments

      :param table: Table remote destination
      :param export_dir: Hive table to export
      :param input_null_string: The string to be interpreted as null for
          string columns
      :param input_null_non_string: The string to be interpreted as null
          for non-string columns
      :param staging_table: The table in which data will be staged before
          being inserted into the destination table
      :param clear_staging_table: Indicate that any data present in the
          staging table can be deleted
      :param enclosed_by: Sets a required field enclosing character
      :param escaped_by: Sets the escape character
      :param input_fields_terminated_by: Sets the field separator character
      :param input_lines_terminated_by: Sets the end-of-line character
      :param input_optionally_enclosed_by: Sets a field enclosing character
      :param batch: Use batch mode for underlying statement execution
      :param relaxed_isolation: Transaction isolation to read uncommitted
          for the mappers
      :param extra_export_options: Extra export options to pass as dict.
          If a key doesn't have a value, just pass an empty string to it.
          Don't include prefix of -- for sqoop options.




