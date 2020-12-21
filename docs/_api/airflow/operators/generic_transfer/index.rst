:mod:`airflow.operators.generic_transfer`
=========================================

.. py:module:: airflow.operators.generic_transfer


Module Contents
---------------

.. py:class:: GenericTransfer(*, sql: str, destination_table: str, source_conn_id: str, destination_conn_id: str, preoperator: Optional[Union[str, List[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Moves data from a connection to another, assuming that they both
   provide the required methods in their respective hooks. The source hook
   needs to expose a `get_records` method, and the destination a
   `insert_rows` method.

   This is meant to be used on small-ish datasets that fit in memory.

   :param sql: SQL query to execute against the source database. (templated)
   :type sql: str
   :param destination_table: target table. (templated)
   :type destination_table: str
   :param source_conn_id: source connection
   :type source_conn_id: str
   :param destination_conn_id: source connection
   :type destination_conn_id: str
   :param preoperator: sql statement or list of statements to be
       executed prior to loading the data. (templated)
   :type preoperator: str or list[str]

   .. attribute:: template_fields
      :annotation: = ['sql', 'destination_table', 'preoperator']

      

   .. attribute:: template_ext
      :annotation: = ['.sql', '.hql']

      

   .. attribute:: ui_color
      :annotation: = #b0f07c

      

   
   .. method:: execute(self, context)




