:mod:`airflow.providers.qubole.hooks.qubole`
============================================

.. py:module:: airflow.providers.qubole.hooks.qubole

.. autoapi-nested-parse::

   Qubole hook



Module Contents
---------------

.. data:: log
   

   

.. data:: COMMAND_CLASSES
   

   

.. data:: POSITIONAL_ARGS
   

   

.. function:: flatten_list(list_of_lists) -> list
   Flatten the list


.. function:: filter_options(options: list) -> list
   Remove options from the list


.. function:: get_options_list(command_class) -> list
   Get options list


.. function:: build_command_args() -> Tuple[Dict[str, list], list]
   Build Command argument from command and options


.. py:class:: QuboleHook(*args, **kwargs)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Hook for Qubole communication

   
   .. staticmethod:: handle_failure_retry(context)

      Handle retries in case of failures



   
   .. method:: execute(self, context)

      Execute call



   
   .. method:: kill(self, ti)

      Kill (cancel) a Qubole command

      :param ti: Task Instance of the dag, used to determine the Quboles command id
      :return: response from Qubole



   
   .. method:: get_results(self, ti=None, fp=None, inline: bool = True, delim=None, fetch: bool = True)

      Get results (or just s3 locations) of a command from Qubole and save into a file

      :param ti: Task Instance of the dag, used to determine the Quboles command id
      :param fp: Optional file pointer, will create one and return if None passed
      :param inline: True to download actual results, False to get s3 locations only
      :param delim: Replaces the CTL-A chars with the given delim, defaults to ','
      :param fetch: when inline is True, get results directly from s3 (if large)
      :return: file location containing actual results or s3 locations of results



   
   .. method:: get_log(self, ti)

      Get Logs of a command from Qubole

      :param ti: Task Instance of the dag, used to determine the Quboles command id
      :return: command log as text



   
   .. method:: get_jobs_id(self, ti)

      Get jobs associated with a Qubole commands

      :param ti: Task Instance of the dag, used to determine the Quboles command id
      :return: Job information associated with command



   
   .. method:: create_cmd_args(self, context)

      Creates command arguments



   
   .. staticmethod:: _add_tags(tags, value)




