:mod:`airflow.providers.exasol.operators.exasol`
================================================

.. py:module:: airflow.providers.exasol.operators.exasol


Module Contents
---------------

.. py:class:: ExasolOperator(*, sql: str, exasol_conn_id: str = 'exasol_default', autocommit: bool = False, parameters: Optional[dict] = None, schema: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes sql code in a specific Exasol database

   :param sql: the sql code to be executed. (templated)
   :type sql: Can receive a str representing a sql statement,
       a list of str (sql statements), or reference to a template file.
       Template reference are recognized by str ending in '.sql'
   :param exasol_conn_id: reference to a specific Exasol database
   :type exasol_conn_id: string
   :param autocommit: if True, each command is automatically committed.
       (default value: False)
   :type autocommit: bool
   :param parameters: (optional) the parameters to render the SQL query with.
   :type parameters: dict
   :param schema: (optional) name of the schema which overwrite defined one in connection
   :type schema: string

   .. attribute:: template_fields
      :annotation: = ['sql']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #ededed

      

   
   .. method:: execute(self, context)




