:mod:`airflow.providers.apache.spark.hooks.spark_jdbc_script`
=============================================================

.. py:module:: airflow.providers.apache.spark.hooks.spark_jdbc_script


Module Contents
---------------

.. data:: SPARK_WRITE_TO_JDBC
   :annotation: :str = spark_to_jdbc

   

.. data:: SPARK_READ_FROM_JDBC
   :annotation: :str = jdbc_to_spark

   

.. function:: set_common_options(spark_source: Any, url: str = 'localhost:5432', jdbc_table: str = 'default.default', user: str = 'root', password: str = 'root', driver: str = 'driver') -> Any
   Get Spark source from JDBC connection

   :param spark_source: Spark source, here is Spark reader or writer
   :param url: JDBC resource url
   :param jdbc_table: JDBC resource table name
   :param user: JDBC resource user name
   :param password: JDBC resource password
   :param driver: JDBC resource driver


.. function:: spark_write_to_jdbc(spark_session: SparkSession, url: str, user: str, password: str, metastore_table: str, jdbc_table: str, driver: Any, truncate: bool, save_mode: str, batch_size: int, num_partitions: int, create_table_column_types: str) -> None
   Transfer data from Spark to JDBC source


.. function:: spark_read_from_jdbc(spark_session: SparkSession, url: str, user: str, password: str, metastore_table: str, jdbc_table: str, driver: Any, save_mode: str, save_format: str, fetch_size: int, num_partitions: int, partition_column: str, lower_bound: str, upper_bound: str) -> None
   Transfer data from JDBC source to Spark


.. function:: _parse_arguments(args: Optional[List[str]] = None) -> Any

.. function:: _create_spark_session(arguments: Any) -> SparkSession

.. function:: _run_spark(arguments: Any) -> None

