:mod:`airflow.utils.process_utils`
==================================

.. py:module:: airflow.utils.process_utils

.. autoapi-nested-parse::

   Utilities for running or stopping processes



Module Contents
---------------

.. data:: log
   

   

.. data:: DEFAULT_TIME_TO_WAIT_AFTER_SIGTERM
   

   

.. function:: reap_process_group(pgid, logger, sig=signal.SIGTERM, timeout=DEFAULT_TIME_TO_WAIT_AFTER_SIGTERM)
   Tries really hard to terminate all processes in the group (including grandchildren). Will send
   sig (SIGTERM) to the process group of pid. If any process is alive after timeout
   a SIGKILL will be send.

   :param pgid: process group id to kill
   :param logger: log handler
   :param sig: signal type
   :param timeout: how much time a process has to terminate


.. function:: execute_in_subprocess(cmd: List[str])
   Execute a process and stream output to logger

   :param cmd: command and arguments to run
   :type cmd: List[str]


.. function:: execute_interactive(cmd: List[str], **kwargs)
   Runs the new command as a subprocess and ensures that the terminal's state is restored to its original
   state after the process is completed e.g. if the subprocess hides the cursor, it will be restored after
   the process is completed.


.. function:: kill_child_processes_by_pids(pids_to_kill: List[int], timeout: int = 5) -> None
   Kills child processes for the current process.

   First, it sends the SIGTERM signal, and after the time specified by the `timeout` parameter, sends
   the SIGKILL signal, if the process is still alive.

   :param pids_to_kill: List of PID to be killed.
   :type pids_to_kill: List[int]
   :param timeout: The time to wait before sending the SIGKILL signal.
   :type timeout: Optional[int]


.. function:: patch_environ(new_env_variables: Dict[str, str])
   Sets environment variables in context. After leaving the context, it restores its original state.

   :param new_env_variables: Environment variables to set


.. function:: check_if_pidfile_process_is_running(pid_file: str, process_name: str)
   Checks if a pidfile already exists and process is still running.
   If process is dead then pidfile is removed.

   :param pid_file: path to the pidfile
   :param process_name: name used in exception if process is up and
       running


