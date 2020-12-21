:mod:`airflow.providers.apache.hive.operators.hive`
===================================================

.. py:module:: airflow.providers.apache.hive.operators.hive


Module Contents
---------------

.. py:class:: HiveOperator(*, hql: str, hive_cli_conn_id: str = 'hive_cli_default', schema: str = 'default', hiveconfs: Optional[Dict[Any, Any]] = None, hiveconf_jinja_translate: bool = False, script_begin_tag: Optional[str] = None, run_as_owner: bool = False, mapred_queue: Optional[str] = None, mapred_queue_priority: Optional[str] = None, mapred_job_name: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes hql code or hive script in a specific Hive database.

   :param hql: the hql to be executed. Note that you may also use
       a relative path from the dag file of a (template) hive
       script. (templated)
   :type hql: str
   :param hive_cli_conn_id: reference to the Hive database. (templated)
   :type hive_cli_conn_id: str
   :param hiveconfs: if defined, these key value pairs will be passed
       to hive as ``-hiveconf "key"="value"``
   :type hiveconfs: dict
   :param hiveconf_jinja_translate: when True, hiveconf-type templating
       ${var} gets translated into jinja-type templating {{ var }} and
       ${hiveconf:var} gets translated into jinja-type templating {{ var }}.
       Note that you may want to use this along with the
       ``DAG(user_defined_macros=myargs)`` parameter. View the DAG
       object documentation for more details.
   :type hiveconf_jinja_translate: bool
   :param script_begin_tag: If defined, the operator will get rid of the
       part of the script before the first occurrence of `script_begin_tag`
   :type script_begin_tag: str
   :param run_as_owner: Run HQL code as a DAG's owner.
   :type run_as_owner: bool
   :param mapred_queue: queue used by the Hadoop CapacityScheduler. (templated)
   :type  mapred_queue: str
   :param mapred_queue_priority: priority within CapacityScheduler queue.
       Possible settings include: VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW
   :type  mapred_queue_priority: str
   :param mapred_job_name: This name will appear in the jobtracker.
       This can make monitoring easier.
   :type  mapred_job_name: str

   .. attribute:: template_fields
      :annotation: = ['hql', 'schema', 'hive_cli_conn_id', 'mapred_queue', 'hiveconfs', 'mapred_job_name', 'mapred_queue_priority']

      

   .. attribute:: template_ext
      :annotation: = ['.hql', '.sql']

      

   .. attribute:: ui_color
      :annotation: = #f0e4ec

      

   
   .. method:: get_hook(self)

      Get Hive cli hook



   
   .. method:: prepare_template(self)



   
   .. method:: execute(self, context: Dict[str, Any])



   
   .. method:: dry_run(self)



   
   .. method:: on_kill(self)



   
   .. method:: clear_airflow_vars(self)

      Reset airflow environment variables to prevent existing ones from impacting behavior.




