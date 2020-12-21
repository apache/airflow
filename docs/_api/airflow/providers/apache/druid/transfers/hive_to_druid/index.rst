:mod:`airflow.providers.apache.druid.transfers.hive_to_druid`
=============================================================

.. py:module:: airflow.providers.apache.druid.transfers.hive_to_druid

.. autoapi-nested-parse::

   This module contains operator to move data from Hive to Druid.



Module Contents
---------------

.. data:: LOAD_CHECK_INTERVAL
   :annotation: = 5

   

.. data:: DEFAULT_TARGET_PARTITION_SIZE
   :annotation: = 5000000

   

.. py:class:: HiveToDruidOperator(*, sql: str, druid_datasource: str, ts_dim: str, metric_spec: Optional[List[Any]] = None, hive_cli_conn_id: str = 'hive_cli_default', druid_ingest_conn_id: str = 'druid_ingest_default', metastore_conn_id: str = 'metastore_default', hadoop_dependency_coordinates: Optional[List[str]] = None, intervals: Optional[List[Any]] = None, num_shards: float = -1, target_partition_size: int = -1, query_granularity: str = 'NONE', segment_granularity: str = 'DAY', hive_tblproperties: Optional[Dict[Any, Any]] = None, job_properties: Optional[Dict[Any, Any]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Moves data from Hive to Druid, [del]note that for now the data is loaded
   into memory before being pushed to Druid, so this operator should
   be used for smallish amount of data.[/del]

   :param sql: SQL query to execute against the Druid database. (templated)
   :type sql: str
   :param druid_datasource: the datasource you want to ingest into in druid
   :type druid_datasource: str
   :param ts_dim: the timestamp dimension
   :type ts_dim: str
   :param metric_spec: the metrics you want to define for your data
   :type metric_spec: list
   :param hive_cli_conn_id: the hive connection id
   :type hive_cli_conn_id: str
   :param druid_ingest_conn_id: the druid ingest connection id
   :type druid_ingest_conn_id: str
   :param metastore_conn_id: the metastore connection id
   :type metastore_conn_id: str
   :param hadoop_dependency_coordinates: list of coordinates to squeeze
       int the ingest json
   :type hadoop_dependency_coordinates: list[str]
   :param intervals: list of time intervals that defines segments,
       this is passed as is to the json object. (templated)
   :type intervals: list
   :param num_shards: Directly specify the number of shards to create.
   :type num_shards: float
   :param target_partition_size: Target number of rows to include in a partition,
   :type target_partition_size: int
   :param query_granularity: The minimum granularity to be able to query results at and the granularity of
       the data inside the segment. E.g. a value of "minute" will mean that data is aggregated at minutely
       granularity. That is, if there are collisions in the tuple (minute(timestamp), dimensions), then it
       will aggregate values together using the aggregators instead of storing individual rows.
       A granularity of 'NONE' means millisecond granularity.
   :type query_granularity: str
   :param segment_granularity: The granularity to create time chunks at. Multiple segments can be created per
       time chunk. For example, with 'DAY' segmentGranularity, the events of the same day fall into the
       same time chunk which can be optionally further partitioned into multiple segments based on other
       configurations and input size.
   :type segment_granularity: str
   :param hive_tblproperties: additional properties for tblproperties in
       hive for the staging table
   :type hive_tblproperties: dict
   :param job_properties: additional properties for job
   :type job_properties: dict

   .. attribute:: template_fields
      :annotation: = ['sql', 'intervals']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   
   .. method:: execute(self, context: Dict[str, Any])



   
   .. method:: construct_ingest_query(self, static_path: str, columns: List[str])

      Builds an ingest query for an HDFS TSV load.

      :param static_path: The path on hdfs where the data is
      :type static_path: str
      :param columns: List of all the columns that are available
      :type columns: list




