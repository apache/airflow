:mod:`airflow.providers.apache.pig.hooks.pig`
=============================================

.. py:module:: airflow.providers.apache.pig.hooks.pig


Module Contents
---------------

.. py:class:: PigCliHook(pig_cli_conn_id: str = 'pig_cli_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Simple wrapper around the pig CLI.

   Note that you can also set default pig CLI properties using the
   ``pig_properties`` to be used in your connection as in
   ``{"pig_properties": "-Dpig.tmpfilecompression=true"}``

   
   .. method:: run_cli(self, pig: str, pig_opts: Optional[str] = None, verbose: bool = True)

      Run an pig script using the pig cli

      >>> ph = PigCliHook()
      >>> result = ph.run_cli("ls /;", pig_opts="-x mapreduce")
      >>> ("hdfs://" in result)
      True



   
   .. method:: kill(self)

      Kill Pig job




