:mod:`airflow.providers.vertica.operators.vertica`
==================================================

.. py:module:: airflow.providers.vertica.operators.vertica


Module Contents
---------------

.. py:class:: VerticaOperator(*, sql: Union[str, List[str]], vertica_conn_id: str = 'vertica_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes sql code in a specific Vertica database.

   :param vertica_conn_id: reference to a specific Vertica database
   :type vertica_conn_id: str
   :param sql: the sql code to be executed. (templated)
   :type sql: Can receive a str representing a sql statement,
       a list of str (sql statements), or reference to a template file.
       Template reference are recognized by str ending in '.sql'

   .. attribute:: template_fields
      :annotation: = ['sql']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #b4e0ff

      

   
   .. method:: execute(self, context: Dict[Any, Any])




