:mod:`airflow.providers.jdbc.operators.jdbc`
============================================

.. py:module:: airflow.providers.jdbc.operators.jdbc


Module Contents
---------------

.. py:class:: JdbcOperator(*, sql: str, jdbc_conn_id: str = 'jdbc_default', autocommit: bool = False, parameters: Optional[Union[Mapping, Iterable]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes sql code in a database using jdbc driver.

   Requires jaydebeapi.

   :param sql: the sql code to be executed. (templated)
   :type sql: Can receive a str representing a sql statement,
       a list of str (sql statements), or reference to a template file.
       Template reference are recognized by str ending in '.sql'
   :param jdbc_conn_id: reference to a predefined database
   :type jdbc_conn_id: str
   :param autocommit: if True, each command is automatically committed.
       (default value: False)
   :type autocommit: bool
   :param parameters: (optional) the parameters to render the SQL query with.
   :type parameters: dict or iterable

   .. attribute:: template_fields
      :annotation: = ['sql']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #ededed

      

   
   .. method:: execute(self, context)




