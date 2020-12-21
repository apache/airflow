:mod:`airflow.providers.apache.hive.hooks.hive`
===============================================

.. py:module:: airflow.providers.apache.hive.hooks.hive


Module Contents
---------------

.. data:: HIVE_QUEUE_PRIORITIES
   :annotation: = ['VERY_HIGH', 'HIGH', 'NORMAL', 'LOW', 'VERY_LOW']

   

.. function:: get_context_from_env_var() -> Dict[Any, Any]
   Extract context from env variable, e.g. dag_id, task_id and execution_date,
   so that they can be used inside BashOperator and PythonOperator.

   :return: The context of interest.


.. py:class:: HiveCliHook(hive_cli_conn_id: str = 'hive_cli_default', run_as: Optional[str] = None, mapred_queue: Optional[str] = None, mapred_queue_priority: Optional[str] = None, mapred_job_name: Optional[str] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Simple wrapper around the hive CLI.

   It also supports the ``beeline``
   a lighter CLI that runs JDBC and is replacing the heavier
   traditional CLI. To enable ``beeline``, set the use_beeline param in the
   extra field of your connection as in ``{ "use_beeline": true }``

   Note that you can also set default hive CLI parameters using the
   ``hive_cli_params`` to be used in your connection as in
   ``{"hive_cli_params": "-hiveconf mapred.job.tracker=some.jobtracker:444"}``
   Parameters passed here can be overridden by run_cli's hive_conf param

   The extra connection parameter ``auth`` gets passed as in the ``jdbc``
   connection string as is.

   :param mapred_queue: queue used by the Hadoop Scheduler (Capacity or Fair)
   :type  mapred_queue: str
   :param mapred_queue_priority: priority within the job queue.
       Possible settings include: VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW
   :type  mapred_queue_priority: str
   :param mapred_job_name: This name will appear in the jobtracker.
       This can make monitoring easier.
   :type  mapred_job_name: str

   
   .. method:: _get_proxy_user(self)

      This function set the proper proxy_user value in case the user overwrite the default.



   
   .. method:: _prepare_cli_cmd(self)

      This function creates the command list from available information



   
   .. staticmethod:: _prepare_hiveconf(d: Dict[Any, Any])

      This function prepares a list of hiveconf params
      from a dictionary of key value pairs.

      :param d:
      :type d: dict

      >>> hh = HiveCliHook()
      >>> hive_conf = {"hive.exec.dynamic.partition": "true",
      ... "hive.exec.dynamic.partition.mode": "nonstrict"}
      >>> hh._prepare_hiveconf(hive_conf)
      ["-hiveconf", "hive.exec.dynamic.partition=true",        "-hiveconf", "hive.exec.dynamic.partition.mode=nonstrict"]



   
   .. method:: run_cli(self, hql: Union[str, Text], schema: Optional[str] = None, verbose: bool = True, hive_conf: Optional[Dict[Any, Any]] = None)

      Run an hql statement using the hive cli. If hive_conf is specified
      it should be a dict and the entries will be set as key/value pairs
      in HiveConf


      :param hive_conf: if specified these key value pairs will be passed
          to hive as ``-hiveconf "key"="value"``. Note that they will be
          passed after the ``hive_cli_params`` and thus will override
          whatever values are specified in the database.
      :type hive_conf: dict

      >>> hh = HiveCliHook()
      >>> result = hh.run_cli("USE airflow;")
      >>> ("OK" in result)
      True



   
   .. method:: test_hql(self, hql: Union[str, Text])

      Test an hql statement using the hive cli and EXPLAIN



   
   .. method:: load_df(self, df: pandas.DataFrame, table: str, field_dict: Optional[Dict[Any, Any]] = None, delimiter: str = ',', encoding: str = 'utf8', pandas_kwargs: Any = None, **kwargs)

      Loads a pandas DataFrame into hive.

      Hive data types will be inferred if not passed but column names will
      not be sanitized.

      :param df: DataFrame to load into a Hive table
      :type df: pandas.DataFrame
      :param table: target Hive table, use dot notation to target a
          specific database
      :type table: str
      :param field_dict: mapping from column name to hive data type.
          Note that it must be OrderedDict so as to keep columns' order.
      :type field_dict: collections.OrderedDict
      :param delimiter: field delimiter in the file
      :type delimiter: str
      :param encoding: str encoding to use when writing DataFrame to file
      :type encoding: str
      :param pandas_kwargs: passed to DataFrame.to_csv
      :type pandas_kwargs: dict
      :param kwargs: passed to self.load_file



   
   .. method:: load_file(self, filepath: str, table: str, delimiter: str = ',', field_dict: Optional[Dict[Any, Any]] = None, create: bool = True, overwrite: bool = True, partition: Optional[Dict[str, Any]] = None, recreate: bool = False, tblproperties: Optional[Dict[str, Any]] = None)

      Loads a local file into Hive

      Note that the table generated in Hive uses ``STORED AS textfile``
      which isn't the most efficient serialization format. If a
      large amount of data is loaded and/or if the tables gets
      queried considerably, you may want to use this operator only to
      stage the data into a temporary table before loading it into its
      final destination using a ``HiveOperator``.

      :param filepath: local filepath of the file to load
      :type filepath: str
      :param table: target Hive table, use dot notation to target a
          specific database
      :type table: str
      :param delimiter: field delimiter in the file
      :type delimiter: str
      :param field_dict: A dictionary of the fields name in the file
          as keys and their Hive types as values.
          Note that it must be OrderedDict so as to keep columns' order.
      :type field_dict: collections.OrderedDict
      :param create: whether to create the table if it doesn't exist
      :type create: bool
      :param overwrite: whether to overwrite the data in table or partition
      :type overwrite: bool
      :param partition: target partition as a dict of partition columns
          and values
      :type partition: dict
      :param recreate: whether to drop and recreate the table at every
          execution
      :type recreate: bool
      :param tblproperties: TBLPROPERTIES of the hive table being created
      :type tblproperties: dict



   
   .. method:: kill(self)

      Kill Hive cli command




.. py:class:: HiveMetastoreHook(metastore_conn_id: str = 'metastore_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Wrapper to interact with the Hive Metastore

   .. attribute:: MAX_PART_COUNT
      :annotation: = 32767

      

   
   .. method:: __getstate__(self)



   
   .. method:: __setstate__(self, d: Dict[str, Any])



   
   .. method:: get_metastore_client(self)

      Returns a Hive thrift client.



   
   .. method:: _find_valid_server(self)



   
   .. method:: get_conn(self)



   
   .. method:: check_for_partition(self, schema: str, table: str, partition: str)

      Checks whether a partition exists

      :param schema: Name of hive schema (database) @table belongs to
      :type schema: str
      :param table: Name of hive table @partition belongs to
      :type schema: str
      :partition: Expression that matches the partitions to check for
          (eg `a = 'b' AND c = 'd'`)
      :type schema: str
      :rtype: bool

      >>> hh = HiveMetastoreHook()
      >>> t = 'static_babynames_partitioned'
      >>> hh.check_for_partition('airflow', t, "ds='2015-01-01'")
      True



   
   .. method:: check_for_named_partition(self, schema: str, table: str, partition_name: str)

      Checks whether a partition with a given name exists

      :param schema: Name of hive schema (database) @table belongs to
      :type schema: str
      :param table: Name of hive table @partition belongs to
      :type table: str
      :partition: Name of the partitions to check for (eg `a=b/c=d`)
      :type table: str
      :rtype: bool

      >>> hh = HiveMetastoreHook()
      >>> t = 'static_babynames_partitioned'
      >>> hh.check_for_named_partition('airflow', t, "ds=2015-01-01")
      True
      >>> hh.check_for_named_partition('airflow', t, "ds=xxx")
      False



   
   .. method:: get_table(self, table_name: str, db: str = 'default')

      Get a metastore table object

      >>> hh = HiveMetastoreHook()
      >>> t = hh.get_table(db='airflow', table_name='static_babynames')
      >>> t.tableName
      'static_babynames'
      >>> [col.name for col in t.sd.cols]
      ['state', 'year', 'name', 'gender', 'num']



   
   .. method:: get_tables(self, db: str, pattern: str = '*')

      Get a metastore table object



   
   .. method:: get_databases(self, pattern: str = '*')

      Get a metastore table object



   
   .. method:: get_partitions(self, schema: str, table_name: str, partition_filter: Optional[str] = None)

      Returns a list of all partitions in a table. Works only
      for tables with less than 32767 (java short max val).
      For subpartitioned table, the number might easily exceed this.

      >>> hh = HiveMetastoreHook()
      >>> t = 'static_babynames_partitioned'
      >>> parts = hh.get_partitions(schema='airflow', table_name=t)
      >>> len(parts)
      1
      >>> parts
      [{'ds': '2015-01-01'}]



   
   .. staticmethod:: _get_max_partition_from_part_specs(part_specs: List[Any], partition_key: Optional[str], filter_map: Optional[Dict[str, Any]])

      Helper method to get max partition of partitions with partition_key
      from part specs. key:value pair in filter_map will be used to
      filter out partitions.

      :param part_specs: list of partition specs.
      :type part_specs: list
      :param partition_key: partition key name.
      :type partition_key: str
      :param filter_map: partition_key:partition_value map used for partition filtering,
                         e.g. {'key1': 'value1', 'key2': 'value2'}.
                         Only partitions matching all partition_key:partition_value
                         pairs will be considered as candidates of max partition.
      :type filter_map: map
      :return: Max partition or None if part_specs is empty.
      :rtype: basestring



   
   .. method:: max_partition(self, schema: str, table_name: str, field: Optional[str] = None, filter_map: Optional[Dict[Any, Any]] = None)

      Returns the maximum value for all partitions with given field in a table.
      If only one partition key exist in the table, the key will be used as field.
      filter_map should be a partition_key:partition_value map and will be used to
      filter out partitions.

      :param schema: schema name.
      :type schema: str
      :param table_name: table name.
      :type table_name: str
      :param field: partition key to get max partition from.
      :type field: str
      :param filter_map: partition_key:partition_value map used for partition filtering.
      :type filter_map: map

      >>> hh = HiveMetastoreHook()
      >>> filter_map = {'ds': '2015-01-01', 'ds': '2014-01-01'}
      >>> t = 'static_babynames_partitioned'
      >>> hh.max_partition(schema='airflow',        ... table_name=t, field='ds', filter_map=filter_map)
      '2015-01-01'



   
   .. method:: table_exists(self, table_name: str, db: str = 'default')

      Check if table exists

      >>> hh = HiveMetastoreHook()
      >>> hh.table_exists(db='airflow', table_name='static_babynames')
      True
      >>> hh.table_exists(db='airflow', table_name='does_not_exist')
      False



   
   .. method:: drop_partitions(self, table_name, part_vals, delete_data=False, db='default')

      Drop partitions from the given table matching the part_vals input

      :param table_name: table name.
      :type table_name: str
      :param part_vals: list of partition specs.
      :type part_vals: list
      :param delete_data: Setting to control if underlying data have to deleted
                          in addition to dropping partitions.
      :type delete_data: bool
      :param db: Name of hive schema (database) @table belongs to
      :type db: str

      >>> hh = HiveMetastoreHook()
      >>> hh.drop_partitions(db='airflow', table_name='static_babynames',
      part_vals="['2020-05-01']")
      True




.. py:class:: HiveServer2Hook

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Wrapper around the pyhive library

   Notes:
   * the default authMechanism is PLAIN, to override it you
   can specify it in the ``extra`` of your connection in the UI
   * the default for run_set_variable_statements is true, if you
   are using impala you may need to set it to false in the
   ``extra`` of your connection in the UI

   .. attribute:: conn_name_attr
      :annotation: = hiveserver2_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = hiveserver2_default

      

   .. attribute:: supports_autocommit
      :annotation: = False

      

   
   .. method:: get_conn(self, schema: Optional[str] = None)

      Returns a Hive connection object.



   
   .. method:: _get_results(self, hql: Union[str, Text, List[str]], schema: str = 'default', fetch_size: Optional[int] = None, hive_conf: Optional[Dict[Any, Any]] = None)



   
   .. method:: get_results(self, hql: Union[str, Text], schema: str = 'default', fetch_size: Optional[int] = None, hive_conf: Optional[Dict[Any, Any]] = None)

      Get results of the provided hql in target schema.

      :param hql: hql to be executed.
      :type hql: str or list
      :param schema: target schema, default to 'default'.
      :type schema: str
      :param fetch_size: max size of result to fetch.
      :type fetch_size: int
      :param hive_conf: hive_conf to execute alone with the hql.
      :type hive_conf: dict
      :return: results of hql execution, dict with data (list of results) and header
      :rtype: dict



   
   .. method:: to_csv(self, hql: Union[str, Text], csv_filepath: str, schema: str = 'default', delimiter: str = ',', lineterminator: str = '\r\n', output_header: bool = True, fetch_size: int = 1000, hive_conf: Optional[Dict[Any, Any]] = None)

      Execute hql in target schema and write results to a csv file.

      :param hql: hql to be executed.
      :type hql: str or list
      :param csv_filepath: filepath of csv to write results into.
      :type csv_filepath: str
      :param schema: target schema, default to 'default'.
      :type schema: str
      :param delimiter: delimiter of the csv file, default to ','.
      :type delimiter: str
      :param lineterminator: lineterminator of the csv file.
      :type lineterminator: str
      :param output_header: header of the csv file, default to True.
      :type output_header: bool
      :param fetch_size: number of result rows to write into the csv file, default to 1000.
      :type fetch_size: int
      :param hive_conf: hive_conf to execute alone with the hql.
      :type hive_conf: dict



   
   .. method:: get_records(self, hql: Union[str, Text], schema: str = 'default', hive_conf: Optional[Dict[Any, Any]] = None)

      Get a set of records from a Hive query.

      :param hql: hql to be executed.
      :type hql: str or list
      :param schema: target schema, default to 'default'.
      :type schema: str
      :param hive_conf: hive_conf to execute alone with the hql.
      :type hive_conf: dict
      :return: result of hive execution
      :rtype: list

      >>> hh = HiveServer2Hook()
      >>> sql = "SELECT * FROM airflow.static_babynames LIMIT 100"
      >>> len(hh.get_records(sql))
      100



   
   .. method:: get_pandas_df(self, hql: Union[str, Text], schema: str = 'default', hive_conf: Optional[Dict[Any, Any]] = None, **kwargs)

      Get a pandas dataframe from a Hive query

      :param hql: hql to be executed.
      :type hql: str or list
      :param schema: target schema, default to 'default'.
      :type schema: str
      :param hive_conf: hive_conf to execute alone with the hql.
      :type hive_conf: dict
      :param kwargs: (optional) passed into pandas.DataFrame constructor
      :type kwargs: dict
      :return: result of hive execution
      :rtype: DataFrame

      >>> hh = HiveServer2Hook()
      >>> sql = "SELECT * FROM airflow.static_babynames LIMIT 100"
      >>> df = hh.get_pandas_df(sql)
      >>> len(df.index)
      100

      :return: pandas.DateFrame




