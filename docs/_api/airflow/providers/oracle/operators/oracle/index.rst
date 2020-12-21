:mod:`airflow.providers.oracle.operators.oracle`
================================================

.. py:module:: airflow.providers.oracle.operators.oracle


Module Contents
---------------

.. py:class:: OracleOperator(*, sql: str, oracle_conn_id: str = 'oracle_default', parameters: Optional[Union[Mapping, Iterable]] = None, autocommit: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes sql code in a specific Oracle database

   :param sql: the sql code to be executed. Can receive a str representing a sql statement,
       a list of str (sql statements), or reference to a template file.
       Template reference are recognized by str ending in '.sql'
       (templated)
   :type sql: str or list[str]
   :param oracle_conn_id: reference to a specific Oracle database
   :type oracle_conn_id: str
   :param parameters: (optional) the parameters to render the SQL query with.
   :type parameters: dict or iterable
   :param autocommit: if True, each command is automatically committed.
       (default value: False)
   :type autocommit: bool

   .. attribute:: template_fields
      :annotation: = ['sql']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #ededed

      

   
   .. method:: execute(self, context)




