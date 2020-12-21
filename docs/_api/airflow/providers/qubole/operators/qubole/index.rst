:mod:`airflow.providers.qubole.operators.qubole`
================================================

.. py:module:: airflow.providers.qubole.operators.qubole

.. autoapi-nested-parse::

   Qubole operator



Module Contents
---------------

.. py:class:: QDSLink

   Bases: :class:`airflow.models.BaseOperatorLink`

   Link to QDS

   .. attribute:: name
      :annotation: = Go to QDS

      

   
   .. method:: get_link(self, operator: BaseOperator, dttm: datetime)

      Get link to qubole command result page.

      :param operator: operator
      :param dttm: datetime
      :return: url link




.. py:class:: QuboleOperator(*, qubole_conn_id: str = 'qubole_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Execute tasks (commands) on QDS (https://qubole.com).

   :param qubole_conn_id: Connection id which consists of qds auth_token
   :type qubole_conn_id: str

   kwargs:
       :command_type: type of command to be executed, e.g. hivecmd, shellcmd, hadoopcmd
       :tags: array of tags to be assigned with the command
       :cluster_label: cluster label on which the command will be executed
       :name: name to be given to command
       :notify: whether to send email on command completion or not (default is False)

       **Arguments specific to command types**

       hivecmd:
           :query: inline query statement
           :script_location: s3 location containing query statement
           :sample_size: size of sample in bytes on which to run query
           :macros: macro values which were used in query
           :sample_size: size of sample in bytes on which to run query
           :hive-version: Specifies the hive version to be used. eg: 0.13,1.2,etc.
       prestocmd:
           :query: inline query statement
           :script_location: s3 location containing query statement
           :macros: macro values which were used in query
       hadoopcmd:
           :sub_commnad: must be one these ["jar", "s3distcp", "streaming"] followed by
               1 or more args
       shellcmd:
           :script: inline command with args
           :script_location: s3 location containing query statement
           :files: list of files in s3 bucket as file1,file2 format. These files will be
               copied into the working directory where the qubole command is being
               executed.
           :archives: list of archives in s3 bucket as archive1,archive2 format. These
               will be unarchived into the working directory where the qubole command is
               being executed
           :parameters: any extra args which need to be passed to script (only when
               script_location is supplied)
       pigcmd:
           :script: inline query statement (latin_statements)
           :script_location: s3 location containing pig query
           :parameters: any extra args which need to be passed to script (only when
               script_location is supplied
       sparkcmd:
           :program: the complete Spark Program in Scala, R, or Python
           :cmdline: spark-submit command line, all required arguments must be specify
               in cmdline itself.
           :sql: inline sql query
           :script_location: s3 location containing query statement
           :language: language of the program, Scala, R, or Python
           :app_id: ID of an Spark job server app
           :arguments: spark-submit command line arguments.
               If `cmdline` is selected, this should not be used because all
               required arguments and configurations are to be passed in the `cmdline` itself.
           :user_program_arguments: arguments that the user program takes in
           :macros: macro values which were used in query
           :note_id: Id of the Notebook to run
       dbtapquerycmd:
           :db_tap_id: data store ID of the target database, in Qubole.
           :query: inline query statement
           :macros: macro values which were used in query
       dbexportcmd:
           :mode: Can be 1 for Hive export or 2 for HDFS/S3 export
           :schema: Db schema name assumed accordingly by database if not specified
           :hive_table: Name of the hive table
           :partition_spec: partition specification for Hive table.
           :dbtap_id: data store ID of the target database, in Qubole.
           :db_table: name of the db table
           :db_update_mode: allowinsert or updateonly
           :db_update_keys: columns used to determine the uniqueness of rows
           :export_dir: HDFS/S3 location from which data will be exported.
           :fields_terminated_by: hex of the char used as column separator in the dataset
           :use_customer_cluster: To use cluster to run command
           :customer_cluster_label: the label of the cluster to run the command on
           :additional_options: Additional Sqoop options which are needed enclose options in
               double or single quotes e.g. '--map-column-hive id=int,data=string'
       dbimportcmd:
           :mode: 1 (simple), 2 (advance)
           :hive_table: Name of the hive table
           :schema: Db schema name assumed accordingly by database if not specified
           :hive_serde: Output format of the Hive Table
           :dbtap_id: data store ID of the target database, in Qubole.
           :db_table: name of the db table
           :where_clause: where clause, if any
           :parallelism: number of parallel db connections to use for extracting data
           :extract_query: SQL query to extract data from db. $CONDITIONS must be part
               of the where clause.
           :boundary_query: Query to be used get range of row IDs to be extracted
           :split_column: Column used as row ID to split data into ranges (mode 2)
           :use_customer_cluster: To use cluster to run command
           :customer_cluster_label: the label of the cluster to run the command on
           :additional_options: Additional Sqoop options which are needed enclose options in
               double or single quotes
       jupytercmd:
           :path: Path including name of the Jupyter notebook to be run with extension.
           :arguments: Valid JSON to be sent to the notebook. Specify the parameters in notebooks and pass
               the parameter value using the JSON format. key is the parameter’s name and value is
               the parameter’s value. Supported types in parameters are string, integer, float and boolean.

   .. note:

       Following fields are template-supported : ``query``, ``script_location``,
       ``sub_command``, ``script``, ``files``, ``archives``, ``program``, ``cmdline``,
       ``sql``, ``where_clause``, ``extract_query``, ``boundary_query``, ``macros``,
       ``tags``, ``name``, ``parameters``, ``dbtap_id``, ``hive_table``, ``db_table``,
       ``split_column``, ``note_id``, ``db_update_keys``, ``export_dir``,
       ``partition_spec``, ``qubole_conn_id``, ``arguments``, ``user_program_arguments``.
       You can also use ``.txt`` files for template driven use cases.

   .. note:

       In QuboleOperator there is a default handler for task failures and retries,
       which generally kills the command running at QDS for the corresponding task
       instance. You can override this behavior by providing your own failure and retry
       handler in task definition.

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['query', 'script_location', 'sub_command', 'script', 'files', 'archives', 'program', 'cmdline', 'sql', 'where_clause', 'tags', 'extract_query', 'boundary_query', 'macros', 'name', 'parameters', 'dbtap_id', 'hive_table', 'db_table', 'split_column', 'note_id', 'db_update_keys', 'export_dir', 'partition_spec', 'qubole_conn_id', 'arguments', 'user_program_arguments', 'cluster_label']

      

   .. attribute:: template_ext
      :annotation: :Iterable[str] = ['.txt']

      

   .. attribute:: ui_color
      :annotation: = #3064A1

      

   .. attribute:: ui_fgcolor
      :annotation: = #fff

      

   .. attribute:: qubole_hook_allowed_args_list
      :annotation: = ['command_type', 'qubole_conn_id', 'fetch_logs']

      

   .. attribute:: operator_extra_links
      

      

   
   .. method:: _get_filtered_args(self, all_kwargs)



   
   .. method:: execute(self, context)



   
   .. method:: on_kill(self, ti=None)



   
   .. method:: get_results(self, ti=None, fp=None, inline: bool = True, delim=None, fetch: bool = True)

      get_results from Qubole



   
   .. method:: get_log(self, ti)

      get_log from Qubole



   
   .. method:: get_jobs_id(self, ti)

      Get jobs_id from Qubole



   
   .. method:: get_hook(self)

      Reinitialising the hook, as some template fields might have changed



   
   .. method:: __getattribute__(self, name: str)



   
   .. method:: __setattr__(self, name: str, value: str)




