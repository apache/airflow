:mod:`airflow.providers.apache.sqoop.operators.sqoop`
=====================================================

.. py:module:: airflow.providers.apache.sqoop.operators.sqoop

.. autoapi-nested-parse::

   This module contains a sqoop 1 operator



Module Contents
---------------

.. py:class:: SqoopOperator(*, conn_id: str = 'sqoop_default', cmd_type: str = 'import', table: Optional[str] = None, query: Optional[str] = None, target_dir: Optional[str] = None, append: bool = False, file_type: str = 'text', columns: Optional[str] = None, num_mappers: Optional[int] = None, split_by: Optional[str] = None, where: Optional[str] = None, export_dir: Optional[str] = None, input_null_string: Optional[str] = None, input_null_non_string: Optional[str] = None, staging_table: Optional[str] = None, clear_staging_table: bool = False, enclosed_by: Optional[str] = None, escaped_by: Optional[str] = None, input_fields_terminated_by: Optional[str] = None, input_lines_terminated_by: Optional[str] = None, input_optionally_enclosed_by: Optional[str] = None, batch: bool = False, direct: bool = False, driver: Optional[Any] = None, verbose: bool = False, relaxed_isolation: bool = False, properties: Optional[Dict[str, Any]] = None, hcatalog_database: Optional[str] = None, hcatalog_table: Optional[str] = None, create_hcatalog_table: bool = False, extra_import_options: Optional[Dict[str, Any]] = None, extra_export_options: Optional[Dict[str, Any]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Execute a Sqoop job.
   Documentation for Apache Sqoop can be found here:
   https://sqoop.apache.org/docs/1.4.2/SqoopUserGuide.html

   :param conn_id: str
   :param cmd_type: str specify command to execute "export" or "import"
   :param table: Table to read
   :param query: Import result of arbitrary SQL query. Instead of using the table,
       columns and where arguments, you can specify a SQL statement with the query
       argument. Must also specify a destination directory with target_dir.
   :param target_dir: HDFS destination directory where the data
       from the rdbms will be written
   :param append: Append data to an existing dataset in HDFS
   :param file_type: "avro", "sequence", "text" Imports data to
       into the specified format. Defaults to text.
   :param columns: <col,col,col> Columns to import from table
   :param num_mappers: Use n mapper tasks to import/export in parallel
   :param split_by: Column of the table used to split work units
   :param where: WHERE clause to use during import
   :param export_dir: HDFS Hive database directory to export to the rdbms
   :param input_null_string: The string to be interpreted as null
       for string columns
   :param input_null_non_string: The string to be interpreted as null
       for non-string columns
   :param staging_table: The table in which data will be staged before
       being inserted into the destination table
   :param clear_staging_table: Indicate that any data present in the
       staging table can be deleted
   :param enclosed_by: Sets a required field enclosing character
   :param escaped_by: Sets the escape character
   :param input_fields_terminated_by: Sets the input field separator
   :param input_lines_terminated_by: Sets the input end-of-line character
   :param input_optionally_enclosed_by: Sets a field enclosing character
   :param batch: Use batch mode for underlying statement execution
   :param direct: Use direct export fast path
   :param driver: Manually specify JDBC driver class to use
   :param verbose: Switch to more verbose logging for debug purposes
   :param relaxed_isolation: use read uncommitted isolation level
   :param hcatalog_database: Specifies the database name for the HCatalog table
   :param hcatalog_table: The argument value for this option is the HCatalog table
   :param create_hcatalog_table: Have sqoop create the hcatalog table passed
       in or not
   :param properties: additional JVM properties passed to sqoop
   :param extra_import_options: Extra import options to pass as dict.
       If a key doesn't have a value, just pass an empty string to it.
       Don't include prefix of -- for sqoop options.
   :param extra_export_options: Extra export options to pass as dict.
       If a key doesn't have a value, just pass an empty string to it.
       Don't include prefix of -- for sqoop options.

   .. attribute:: template_fields
      :annotation: = ['conn_id', 'cmd_type', 'table', 'query', 'target_dir', 'file_type', 'columns', 'split_by', 'where', 'export_dir', 'input_null_string', 'input_null_non_string', 'staging_table', 'enclosed_by', 'escaped_by', 'input_fields_terminated_by', 'input_lines_terminated_by', 'input_optionally_enclosed_by', 'properties', 'extra_import_options', 'driver', 'extra_export_options', 'hcatalog_database', 'hcatalog_table']

      

   .. attribute:: ui_color
      :annotation: = #7D8CA4

      

   
   .. method:: execute(self, context: Dict[str, Any])

      Execute sqoop job



   
   .. method:: on_kill(self)



   
   .. method:: _get_hook(self)




