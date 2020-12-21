:mod:`airflow.cli.commands.webserver_command`
=============================================

.. py:module:: airflow.cli.commands.webserver_command

.. autoapi-nested-parse::

   Webserver command



Module Contents
---------------

.. data:: log
   

   

.. py:class:: GunicornMonitor(gunicorn_master_pid: int, num_workers_expected: int, master_timeout: int, worker_refresh_interval: int, worker_refresh_batch_size: int, reload_on_plugin_change: bool)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Runs forever, monitoring the child processes of @gunicorn_master_proc and
   restarting workers occasionally or when files in the plug-in directory
   has been modified.

   Each iteration of the loop traverses one edge of this state transition
   diagram, where each state (node) represents
   [ num_ready_workers_running / num_workers_running ]. We expect most time to
   be spent in [n / n]. `bs` is the setting webserver.worker_refresh_batch_size.
   The horizontal transition at ? happens after the new worker parses all the
   dags (so it could take a while!)
      V ────────────────────────────────────────────────────────────────────────┐
   [n / n] ──TTIN──> [ [n, n+bs) / n + bs ]  ────?───> [n + bs / n + bs] ──TTOU─┘
      ^                          ^───────────────┘
      │
      │      ┌────────────────v
      └──────┴────── [ [0, n) / n ] <─── start
   We change the number of workers by sending TTIN and TTOU to the gunicorn
   master process, which increases and decreases the number of child workers
   respectively. Gunicorn guarantees that on TTOU workers are terminated
   gracefully and that the oldest worker is terminated.

   :param gunicorn_master_pid: PID for the main Gunicorn process
   :param num_workers_expected: Number of workers to run the Gunicorn web server
   :param master_timeout: Number of seconds the webserver waits before killing gunicorn master that
       doesn't respond
   :param worker_refresh_interval: Number of seconds to wait before refreshing a batch of workers.
   :param worker_refresh_batch_size: Number of workers to refresh at a time. When set to 0, worker
       refresh is disabled. When nonzero, airflow periodically refreshes webserver workers by
       bringing up new ones and killing old ones.
   :param reload_on_plugin_change: If set to True, Airflow will track files in plugins_folder directory.
       When it detects changes, then reload the gunicorn.

   
   .. method:: _generate_plugin_state(self)

      Generate dict of filenames and last modification time of all files in settings.PLUGINS_FOLDER
      directory.



   
   .. staticmethod:: _get_file_hash(fname: str)

      Calculate MD5 hash for file



   
   .. method:: _get_num_ready_workers_running(self)

      Returns number of ready Gunicorn workers by looking for READY_PREFIX in process name



   
   .. method:: _get_num_workers_running(self)

      Returns number of running Gunicorn workers processes



   
   .. method:: _wait_until_true(self, fn, timeout: int = 0)

      Sleeps until fn is true



   
   .. method:: _spawn_new_workers(self, count: int)

      Send signal to kill the worker.

      :param count: The number of workers to spawn



   
   .. method:: _kill_old_workers(self, count: int)

      Send signal to kill the worker.

      :param count: The number of workers to kill



   
   .. method:: _reload_gunicorn(self)

      Send signal to reload the gunciron configuration. When gunciorn receive signals, it reload the
      configuration, start the new worker processes with a new configuration and gracefully
      shutdown older workers.



   
   .. method:: start(self)

      Starts monitoring the webserver.



   
   .. method:: _check_workers(self)




.. function:: webserver(args)
   Starts Airflow Webserver


