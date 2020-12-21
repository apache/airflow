:mod:`airflow.providers.apache.hive.operators.hive_stats`
=========================================================

.. py:module:: airflow.providers.apache.hive.operators.hive_stats


Module Contents
---------------

.. py:class:: HiveStatsCollectionOperator(*, table: str, partition: Any, extra_exprs: Optional[Dict[str, Any]] = None, excluded_columns: Optional[List[str]] = None, assignment_func: Optional[Callable[[str, str], Optional[Dict[Any, Any]]]] = None, metastore_conn_id: str = 'metastore_default', presto_conn_id: str = 'presto_default', mysql_conn_id: str = 'airflow_db', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Gathers partition statistics using a dynamically generated Presto
   query, inserts the stats into a MySql table with this format. Stats
   overwrite themselves if you rerun the same date/partition. ::

       CREATE TABLE hive_stats (
           ds VARCHAR(16),
           table_name VARCHAR(500),
           metric VARCHAR(200),
           value BIGINT
       );

   :param table: the source table, in the format ``database.table_name``. (templated)
   :type table: str
   :param partition: the source partition. (templated)
   :type partition: dict of {col:value}
   :param extra_exprs: dict of expression to run against the table where
       keys are metric names and values are Presto compatible expressions
   :type extra_exprs: dict
   :param excluded_columns: list of columns to exclude, consider
       excluding blobs, large json columns, ...
   :type excluded_columns: list
   :param assignment_func: a function that receives a column name and
       a type, and returns a dict of metric names and an Presto expressions.
       If None is returned, the global defaults are applied. If an
       empty dictionary is returned, no stats are computed for that
       column.
   :type assignment_func: function

   .. attribute:: template_fields
      :annotation: = ['table', 'partition', 'ds', 'dttm']

      

   .. attribute:: ui_color
      :annotation: = #aff7a6

      

   
   .. method:: get_default_exprs(self, col: str, col_type: str)

      Get default expressions



   
   .. method:: execute(self, context: Optional[Dict[str, Any]] = None)




