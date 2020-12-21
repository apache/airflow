:mod:`airflow.providers.apache.druid.operators.druid_check`
===========================================================

.. py:module:: airflow.providers.apache.druid.operators.druid_check


Module Contents
---------------

.. py:class:: DruidCheckOperator(*, sql: str, druid_broker_conn_id: str = 'druid_broker_default', **kwargs)

   Bases: :class:`airflow.operators.check_operator.CheckOperator`

   Performs checks against Druid. The ``DruidCheckOperator`` expects
   a sql query that will return a single row. Each value on that
   first row is evaluated using python ``bool`` casting. If any of the
   values return ``False`` the check is failed and errors out.

   Note that Python bool casting evals the following as ``False``:

   * ``False``
   * ``0``
   * Empty string (``""``)
   * Empty list (``[]``)
   * Empty dictionary or set (``{}``)

   Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
   the count ``== 0``. You can craft much more complex query that could,
   for instance, check that the table has the same number of rows as
   the source table upstream, or that the count of today's partition is
   greater than yesterday's partition, or that a set of metrics are less
   than 3 standard deviation for the 7 day average.
   This operator can be used as a data quality check in your pipeline, and
   depending on where you put it in your DAG, you have the choice to
   stop the critical path, preventing from
   publishing dubious data, or on the side and receive email alerts
   without stopping the progress of the DAG.

   :param sql: the sql to be executed
   :type sql: str
   :param druid_broker_conn_id: reference to the druid broker
   :type druid_broker_conn_id: str

   
   .. method:: get_db_hook(self)

      Return the druid db api hook.



   
   .. method:: get_first(self, sql: str)

      Executes the druid sql to druid broker and returns the first resulting row.

      :param sql: the sql statement to be executed (str)
      :type sql: str



   
   .. method:: execute(self, context: Optional[Dict[Any, Any]] = None)




