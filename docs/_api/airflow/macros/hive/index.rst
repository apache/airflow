:mod:`airflow.macros.hive`
==========================

.. py:module:: airflow.macros.hive


Module Contents
---------------

.. function:: max_partition(table, schema='default', field=None, filter_map=None, metastore_conn_id='metastore_default')
   Gets the max partition for a table.

   :param schema: The hive schema the table lives in
   :type schema: str
   :param table: The hive table you are interested in, supports the dot
       notation as in "my_database.my_table", if a dot is found,
       the schema param is disregarded
   :type table: str
   :param metastore_conn_id: The hive connection you are interested in.
       If your default is set you don't need to use this parameter.
   :type metastore_conn_id: str
   :param filter_map: partition_key:partition_value map used for partition filtering,
                      e.g. {'key1': 'value1', 'key2': 'value2'}.
                      Only partitions matching all partition_key:partition_value
                      pairs will be considered as candidates of max partition.
   :type filter_map: dict
   :param field: the field to get the max value from. If there's only
       one partition field, this will be inferred
   :type field: str

   >>> max_partition('airflow.static_babynames_partitioned')
   '2015-01-01'


.. function:: _closest_date(target_dt, date_list, before_target=None)
   This function finds the date in a list closest to the target date.
   An optional parameter can be given to get the closest before or after.

   :param target_dt: The target date
   :type target_dt: datetime.date
   :param date_list: The list of dates to search
   :type date_list: list[datetime.date]
   :param before_target: closest before or after the target
   :type before_target: bool or None
   :returns: The closest date
   :rtype: datetime.date or None


.. function:: closest_ds_partition(table, ds, before=True, schema='default', metastore_conn_id='metastore_default')
   This function finds the date in a list closest to the target date.
   An optional parameter can be given to get the closest before or after.

   :param table: A hive table name
   :type table: str
   :param ds: A datestamp ``%Y-%m-%d`` e.g. ``yyyy-mm-dd``
   :type ds: list[datetime.date]
   :param before: closest before (True), after (False) or either side of ds
   :type before: bool or None
   :param schema: table schema
   :type schema: str
   :param metastore_conn_id: which metastore connection to use
   :type metastore_conn_id: str
   :returns: The closest date
   :rtype: str or None

   >>> tbl = 'airflow.static_babynames_partitioned'
   >>> closest_ds_partition(tbl, '2015-01-02')
   '2015-01-01'


