:mod:`airflow.providers.postgres.operators.postgres`
====================================================

.. py:module:: airflow.providers.postgres.operators.postgres


Module Contents
---------------

.. py:class:: PostgresOperator(*, sql: str, postgres_conn_id: str = 'postgres_default', autocommit: bool = False, parameters: Optional[Union[Mapping, Iterable]] = None, database: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes sql code in a specific Postgres database

   :param sql: the sql code to be executed. (templated)
   :type sql: Can receive a str representing a sql statement,
       a list of str (sql statements), or reference to a template file.
       Template reference are recognized by str ending in '.sql'
   :param postgres_conn_id: reference to a specific postgres database
   :type postgres_conn_id: str
   :param autocommit: if True, each command is automatically committed.
       (default value: False)
   :type autocommit: bool
   :param parameters: (optional) the parameters to render the SQL query with.
   :type parameters: dict or iterable
   :param database: name of database which overwrite defined one in connection
   :type database: str

   .. attribute:: template_fields
      :annotation: = ['sql']

      

   .. attribute:: template_fields_renderers
      

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #ededed

      

   
   .. method:: execute(self, context)




