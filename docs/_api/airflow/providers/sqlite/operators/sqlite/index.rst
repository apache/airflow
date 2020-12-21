:mod:`airflow.providers.sqlite.operators.sqlite`
================================================

.. py:module:: airflow.providers.sqlite.operators.sqlite


Module Contents
---------------

.. py:class:: SqliteOperator(*, sql: str, sqlite_conn_id: str = 'sqlite_default', parameters: Optional[Union[Mapping, Iterable]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes sql code in a specific Sqlite database

   :param sql: the sql code to be executed. (templated)
   :type sql: str or string pointing to a template file. File must have
       a '.sql' extensions.
   :param sqlite_conn_id: reference to a specific sqlite database
   :type sqlite_conn_id: str
   :param parameters: (optional) the parameters to render the SQL query with.
   :type parameters: dict or iterable

   .. attribute:: template_fields
      :annotation: = ['sql']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #cdaaed

      

   
   .. method:: execute(self, context: Mapping[Any, Any])




