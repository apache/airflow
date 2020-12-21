:mod:`airflow.providers.mysql.operators.mysql`
==============================================

.. py:module:: airflow.providers.mysql.operators.mysql


Module Contents
---------------

.. py:class:: MySqlOperator(*, sql: str, mysql_conn_id: str = 'mysql_default', parameters: Optional[Union[Mapping, Iterable]] = None, autocommit: bool = False, database: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes sql code in a specific MySQL database

   :param sql: the sql code to be executed. Can receive a str representing a
       sql statement, a list of str (sql statements), or reference to a template file.
       Template reference are recognized by str ending in '.sql'
       (templated)
   :type sql: str or list[str]
   :param mysql_conn_id: reference to a specific mysql database
   :type mysql_conn_id: str
   :param parameters: (optional) the parameters to render the SQL query with.
   :type parameters: dict or iterable
   :param autocommit: if True, each command is automatically committed.
       (default value: False)
   :type autocommit: bool
   :param database: name of database which overwrite defined one in connection
   :type database: str

   .. attribute:: template_fields
      :annotation: = ['sql']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #ededed

      

   
   .. method:: execute(self, context: Dict)




