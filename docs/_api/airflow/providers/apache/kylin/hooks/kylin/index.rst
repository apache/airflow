:mod:`airflow.providers.apache.kylin.hooks.kylin`
=================================================

.. py:module:: airflow.providers.apache.kylin.hooks.kylin


Module Contents
---------------

.. py:class:: KylinHook(kylin_conn_id: str = 'kylin_default', project: Optional[str] = None, dsn: Optional[str] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   :param kylin_conn_id: The connection id as configured in Airflow administration.
   :type kylin_conn_id: str
   :param project: project name
   :type project: Optional[str]
   :param dsn: dsn
   :type dsn: Optional[str]

   
   .. method:: get_conn(self)



   
   .. method:: cube_run(self, datasource_name, op, **op_args)

      Run CubeSource command which in CubeSource.support_invoke_command

      :param datasource_name:
      :param op: command
      :param op_args: command args
      :return: response



   
   .. method:: get_job_status(self, job_id)

      Get job status

      :param job_id: kylin job id
      :return: job status




