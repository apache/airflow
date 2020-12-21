:mod:`airflow.providers.apache.pig.operators.pig`
=================================================

.. py:module:: airflow.providers.apache.pig.operators.pig


Module Contents
---------------

.. py:class:: PigOperator(*, pig: str, pig_cli_conn_id: str = 'pig_cli_default', pigparams_jinja_translate: bool = False, pig_opts: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes pig script.

   :param pig: the pig latin script to be executed. (templated)
   :type pig: str
   :param pig_cli_conn_id: reference to the Hive database
   :type pig_cli_conn_id: str
   :param pigparams_jinja_translate: when True, pig params-type templating
       ${var} gets translated into jinja-type templating {{ var }}. Note that
       you may want to use this along with the
       ``DAG(user_defined_macros=myargs)`` parameter. View the DAG
       object documentation for more details.
   :type pigparams_jinja_translate: bool
   :param pig_opts: pig options, such as: -x tez, -useHCatalog, ...
   :type pig_opts: str

   .. attribute:: template_fields
      :annotation: = ['pig']

      

   .. attribute:: template_ext
      :annotation: = ['.pig', '.piglatin']

      

   .. attribute:: ui_color
      :annotation: = #f0e4ec

      

   
   .. method:: prepare_template(self)



   
   .. method:: execute(self, context)



   
   .. method:: on_kill(self)




