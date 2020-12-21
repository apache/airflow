:mod:`airflow.providers.apache.kylin.operators.kylin_cube`
==========================================================

.. py:module:: airflow.providers.apache.kylin.operators.kylin_cube


Module Contents
---------------

.. py:class:: KylinCubeOperator(*, kylin_conn_id: str = 'kylin_default', project: Optional[str] = None, cube: Optional[str] = None, dsn: Optional[str] = None, command: Optional[str] = None, start_time: Optional[str] = None, end_time: Optional[str] = None, offset_start: Optional[str] = None, offset_end: Optional[str] = None, segment_name: Optional[str] = None, is_track_job: bool = False, interval: int = 60, timeout: int = 60 * 60 * 24, eager_error_status=('ERROR', 'DISCARDED', 'KILLED', 'SUICIDAL', 'STOPPED'), **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This operator is used to submit request about kylin build/refresh/merge,
   and can track job status . so users can easier to build kylin job

   For more detail information in
   `Apache Kylin <http://kylin.apache.org/>`_

   :param kylin_conn_id: The connection id as configured in Airflow administration.
   :type kylin_conn_id: str
   :param project: kylin project name, this param will overwrite the project in kylin_conn_id:
   :type project: str
   :param cube: kylin cube name
   :type cube: str
   :param dsn: (dsn , dsn url of kylin connection ,which will overwrite kylin_conn_id.
       for example: kylin://ADMIN:KYLIN@sandbox/learn_kylin?timeout=60&is_debug=1)
   :type dsn: str
   :param command: (kylin command include 'build', 'merge', 'refresh', 'delete',
       'build_streaming', 'merge_streaming', 'refresh_streaming', 'disable', 'enable',
       'purge', 'clone', 'drop'.
       build - use /kylin/api/cubes/{cubeName}/build rest api,and buildType is ‘BUILD’,
       and you should give start_time and end_time
       refresh - use build rest api,and buildType is ‘REFRESH’
       merge - use build rest api,and buildType is ‘MERGE’
       build_streaming - use /kylin/api/cubes/{cubeName}/build2 rest api,and buildType is ‘BUILD’
       and you should give offset_start and offset_end
       refresh_streaming - use build2 rest api,and buildType is ‘REFRESH’
       merge_streaming - use build2 rest api,and buildType is ‘MERGE’
       delete - delete segment, and you should give segment_name value
       disable - disable cube
       enable - enable cube
       purge - purge cube
       clone - clone cube,new cube name is {cube_name}_clone
       drop - drop cube)
   :type command: str
   :param start_time: build segment start time
   :type start_time: Optional[str]
   :param end_time: build segment end time
   :type end_time: Optional[str]
   :param offset_start: streaming build segment start time
   :type offset_start: Optional[str]
   :param offset_end: streaming build segment end time
   :type offset_end: Optional[str]
   :param segment_name: segment name
   :type segment_name: str
   :param is_track_job: (whether to track job status. if value is True,will track job until
       job status is in("FINISHED", "ERROR", "DISCARDED", "KILLED", "SUICIDAL",
       "STOPPED") or timeout)
   :type is_track_job: bool
   :param interval: track job status,default value is 60s
   :type interval: int
   :param timeout: timeout value,default value is 1 day,60 * 60 * 24 s
   :type timeout: int
   :param eager_error_status: (jobs error status,if job status in this list ,this task will be error.
       default value is tuple(["ERROR", "DISCARDED", "KILLED", "SUICIDAL", "STOPPED"]))
   :type eager_error_status: tuple

   .. attribute:: template_fields
      :annotation: = ['project', 'cube', 'dsn', 'command', 'start_time', 'end_time', 'segment_name', 'offset_start', 'offset_end']

      

   .. attribute:: ui_color
      :annotation: = #E79C46

      

   .. attribute:: build_command
      

      

   .. attribute:: jobs_end_status
      

      

   
   .. method:: execute(self, context)




