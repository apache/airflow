:mod:`airflow.utils.dag_processing`
===================================

.. py:module:: airflow.utils.dag_processing

.. autoapi-nested-parse::

   Processes DAGs.



Module Contents
---------------

.. py:class:: AbstractDagFileProcessorProcess

   Processes a DAG file. See SchedulerJob.process_file() for more details.

   .. attribute:: pid
      

      :return: the PID of the process launched to process the given file


   .. attribute:: exit_code
      

      After the process is finished, this can be called to get the return code
      :return: the exit code of the process
      :rtype: int


   .. attribute:: done
      

      Check if the process launched to process this file is done.
      :return: whether the process is finished running
      :rtype: bool


   .. attribute:: result
      

      A list of simple dags found, and the number of import errors

      :return: result of running SchedulerJob.process_file() if available. Otherwise, none
      :rtype: Optional[Tuple[int, int]]


   .. attribute:: start_time
      

      :return: When this started to process the file
      :rtype: datetime


   .. attribute:: file_path
      

      :return: the path to the file that this is processing
      :rtype: unicode


   .. attribute:: waitable_handle
      

      A "waitable" handle that can be passed to ``multiprocessing.connection.wait()``


   
   .. method:: start(self)

      Launch the process to process the file



   
   .. method:: terminate(self, sigkill: bool = False)

      Terminate (and then kill) the process launched to process the file



   
   .. method:: kill(self)

      Kill the process launched to process the file, and ensure consistent state.




.. py:class:: DagParsingStat

   Bases: :class:`typing.NamedTuple`

   Information on processing progress

   .. attribute:: file_paths
      :annotation: :List[str]

      

   .. attribute:: done
      :annotation: :bool

      

   .. attribute:: all_files_processed
      :annotation: :bool

      


.. py:class:: DagFileStat

   Bases: :class:`typing.NamedTuple`

   Information about single processing of one file

   .. attribute:: num_dags
      :annotation: :int

      

   .. attribute:: import_errors
      :annotation: :int

      

   .. attribute:: last_finish_time
      :annotation: :Optional[datetime]

      

   .. attribute:: last_duration
      :annotation: :Optional[float]

      

   .. attribute:: run_count
      :annotation: :int

      


.. py:class:: DagParsingSignal

   Bases: :class:`enum.Enum`

   All signals sent to parser.

   .. attribute:: AGENT_RUN_ONCE
      :annotation: = agent_run_once

      

   .. attribute:: TERMINATE_MANAGER
      :annotation: = terminate_manager

      

   .. attribute:: END_MANAGER
      :annotation: = end_manager

      


.. py:class:: DagFileProcessorAgent(dag_directory: str, max_runs: int, processor_factory: Callable[[str, List[CallbackRequest], Optional[List[str]], bool], AbstractDagFileProcessorProcess], processor_timeout: timedelta, dag_ids: Optional[List[str]], pickle_dags: bool, async_mode: bool)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`, :class:`airflow.utils.mixins.MultiprocessingStartMethodMixin`

   Agent for DAG file processing. It is responsible for all DAG parsing
   related jobs in scheduler process. Mainly it can spin up DagFileProcessorManager
   in a subprocess, collect DAG parsing results from it and communicate
   signal/DAG parsing stat with it.

   This class runs in the main `airflow scheduler` process.

   :param dag_directory: Directory where DAG definitions are kept. All
       files in file_paths should be under this directory
   :type dag_directory: str
   :param max_runs: The number of times to parse and schedule each file. -1
       for unlimited.
   :type max_runs: int
   :param processor_factory: function that creates processors for DAG
       definition files. Arguments are (dag_definition_path, log_file_path)
   :type processor_factory: ([str, List[CallbackRequest], Optional[List[str]], bool]) -> (
       AbstractDagFileProcessorProcess
   )
   :param processor_timeout: How long to wait before timing out a DAG file processor
   :type processor_timeout: timedelta
   :param dag_ids: if specified, only schedule tasks with these DAG IDs
   :type dag_ids: list[str]
   :param pickle_dags: whether to pickle DAGs.
   :type: pickle_dags: bool
   :param async_mode: Whether to start agent in async mode
   :type async_mode: bool

   .. attribute:: done
      

      Has DagFileProcessorManager ended?


   .. attribute:: all_files_processed
      

      Have all files been processed at least once?


   
   .. method:: start(self)

      Launch DagFileProcessorManager processor and start DAG parsing loop in manager.



   
   .. method:: run_single_parsing_loop(self)

      Should only be used when launched DAG file processor manager in sync mode.
      Send agent heartbeat signal to the manager, requesting that it runs one
      processing "loop".

      Call wait_until_finished to ensure that any launched processors have
      finished before continuing



   
   .. method:: send_callback_to_execute(self, request: CallbackRequest)

      Sends information about the callback to be executed by DagFileProcessor.

      :param request: Callback request to be executed.
      :type request: CallbackRequest



   
   .. method:: send_sla_callback_request_to_execute(self, full_filepath: str, dag_id: str)

      Sends information about the SLA callback to be executed by DagFileProcessor.

      :param full_filepath: DAG File path
      :type full_filepath: str
      :param dag_id: DAG ID
      :type dag_id: str



   
   .. method:: wait_until_finished(self)

      Waits until DAG parsing is finished.



   
   .. staticmethod:: _run_processor_manager(dag_directory: str, max_runs: int, processor_factory: Callable[[str, List[CallbackRequest]], AbstractDagFileProcessorProcess], processor_timeout: timedelta, signal_conn: MultiprocessingConnection, dag_ids: Optional[List[str]], pickle_dags: bool, async_mode: bool)



   
   .. method:: heartbeat(self)

      Check if the DagFileProcessorManager process is alive, and process any pending messages



   
   .. method:: _process_message(self, message)



   
   .. method:: _heartbeat_manager(self)

      Heartbeat DAG file processor and restart it if we are not done.



   
   .. method:: _sync_metadata(self, stat)

      Sync metadata from stat queue and only keep the latest stat.



   
   .. method:: terminate(self)

      Send termination signal to DAG parsing processor manager
      and expect it to terminate all DAG file processors.



   
   .. method:: end(self)

      Terminate (and then kill) the manager process launched.
      :return:




.. py:class:: DagFileProcessorManager(dag_directory: str, max_runs: int, processor_factory: Callable[[str, List[CallbackRequest]], AbstractDagFileProcessorProcess], processor_timeout: timedelta, signal_conn: MultiprocessingConnection, dag_ids: Optional[List[str]], pickle_dags: bool, async_mode: bool = True)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Given a list of DAG definition files, this kicks off several processors
   in parallel to process them and put the results to a multiprocessing.Queue
   for DagFileProcessorAgent to harvest. The parallelism is limited and as the
   processors finish, more are launched. The files are processed over and
   over again, but no more often than the specified interval.

   :param dag_directory: Directory where DAG definitions are kept. All
       files in file_paths should be under this directory
   :type dag_directory: unicode
   :param max_runs: The number of times to parse and schedule each file. -1
       for unlimited.
   :type max_runs: int
   :param processor_factory: function that creates processors for DAG
       definition files. Arguments are (dag_definition_path)
   :type processor_factory: (unicode, unicode, list) -> (AbstractDagFileProcessorProcess)
   :param processor_timeout: How long to wait before timing out a DAG file processor
   :type processor_timeout: timedelta
   :param signal_conn: connection to communicate signal with processor agent.
   :type signal_conn: MultiprocessingConnection
   :param dag_ids: if specified, only schedule tasks with these DAG IDs
   :type dag_ids: list[str]
   :param pickle_dags: whether to pickle DAGs.
   :type pickle_dags: bool
   :param async_mode: whether to start the manager in async mode
   :type async_mode: bool

   .. attribute:: file_paths
      

      

   
   .. method:: register_exit_signals(self)

      Register signals that stop child processes



   
   .. method:: _exit_gracefully(self, signum, frame)

      Helper method to clean up DAG file processors to avoid leaving orphan processes.



   
   .. method:: start(self)

      Use multiple processes to parse and generate tasks for the
      DAGs in parallel. By processing them in separate processes,
      we can get parallelism and isolation from potentially harmful
      user code.



   
   .. method:: _run_parsing_loop(self)



   
   .. method:: _add_callback_to_queue(self, request: CallbackRequest)



   
   .. method:: _refresh_dag_dir(self)

      Refresh file paths from dag dir if we haven't done it for too long.



   
   .. method:: _print_stat(self)

      Occasionally print out stats about how fast the files are getting processed



   
   .. method:: clear_nonexistent_import_errors(self, session)

      Clears import errors for files that no longer exist.

      :param session: session for ORM operations
      :type session: sqlalchemy.orm.session.Session



   
   .. method:: _log_file_processing_stats(self, known_file_paths)

      Print out stats about how files are getting processed.

      :param known_file_paths: a list of file paths that may contain Airflow
          DAG definitions
      :type known_file_paths: list[unicode]
      :return: None



   
   .. method:: get_pid(self, file_path)

      :param file_path: the path to the file that's being processed
      :type file_path: unicode
      :return: the PID of the process processing the given file or None if
          the specified file is not being processed
      :rtype: int



   
   .. method:: get_all_pids(self)

      :return: a list of the PIDs for the processors that are running
      :rtype: List[int]



   
   .. method:: get_last_runtime(self, file_path)

      :param file_path: the path to the file that was processed
      :type file_path: unicode
      :return: the runtime (in seconds) of the process of the last run, or
          None if the file was never processed.
      :rtype: float



   
   .. method:: get_last_dag_count(self, file_path)

      :param file_path: the path to the file that was processed
      :type file_path: unicode
      :return: the number of dags loaded from that file, or None if the file
          was never processed.
      :rtype: int



   
   .. method:: get_last_error_count(self, file_path)

      :param file_path: the path to the file that was processed
      :type file_path: unicode
      :return: the number of import errors from processing, or None if the file
          was never processed.
      :rtype: int



   
   .. method:: get_last_finish_time(self, file_path)

      :param file_path: the path to the file that was processed
      :type file_path: unicode
      :return: the finish time of the process of the last run, or None if the
          file was never processed.
      :rtype: datetime



   
   .. method:: get_start_time(self, file_path)

      :param file_path: the path to the file that's being processed
      :type file_path: unicode
      :return: the start time of the process that's processing the
          specified file or None if the file is not currently being processed
      :rtype: datetime



   
   .. method:: get_run_count(self, file_path)

      :param file_path: the path to the file that's being processed
      :type file_path: unicode
      :return: the number of times the given file has been parsed
      :rtype: int



   
   .. method:: set_file_paths(self, new_file_paths)

      Update this with a new set of paths to DAG definition files.

      :param new_file_paths: list of paths to DAG definition files
      :type new_file_paths: list[unicode]
      :return: None



   
   .. method:: wait_until_finished(self)

      Sleeps until all the processors are done.



   
   .. method:: _collect_results_from_processor(self, processor)



   
   .. method:: collect_results(self)

      Collect the result from any finished DAG processors



   
   .. method:: start_new_processes(self)

      Start more processors if we have enough slots and files to process



   
   .. method:: prepare_file_path_queue(self)

      Generate more file paths to process. Result are saved in _file_path_queue.



   
   .. method:: _find_zombies(self, session)

      Find zombie task instances, which are tasks haven't heartbeated for too long
      and update the current zombie list.



   
   .. method:: _kill_timed_out_processors(self)

      Kill any file processors that timeout to defend against process hangs.



   
   .. method:: max_runs_reached(self)

      :return: whether all file paths have been processed max_runs times



   
   .. method:: terminate(self)

      Stops all running processors
      :return: None



   
   .. method:: end(self)

      Kill all child processes on exit since we don't want to leave
      them as orphaned.



   
   .. method:: emit_metrics(self)

      Emit metrics about dag parsing summary

      This is called once every time around the parsing "loop" - i.e. after
      all files have been parsed.




