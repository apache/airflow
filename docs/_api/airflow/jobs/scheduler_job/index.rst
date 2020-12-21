:mod:`airflow.jobs.scheduler_job`
=================================

.. py:module:: airflow.jobs.scheduler_job


Module Contents
---------------

.. data:: TI
   

   

.. data:: DR
   

   

.. data:: DM
   

   

.. py:class:: DagFileProcessorProcess(file_path: str, pickle_dags: bool, dag_ids: Optional[List[str]], callback_requests: List[CallbackRequest])

   Bases: :class:`airflow.utils.dag_processing.AbstractDagFileProcessorProcess`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`, :class:`airflow.utils.mixins.MultiprocessingStartMethodMixin`

   Runs DAG processing in a separate process using DagFileProcessor

   :param file_path: a Python file containing Airflow DAG definitions
   :type file_path: str
   :param pickle_dags: whether to serialize the DAG objects to the DB
   :type pickle_dags: bool
   :param dag_ids: If specified, only look at these DAG ID's
   :type dag_ids: List[str]
   :param callback_requests: failure callback to execute
   :type callback_requests: List[airflow.utils.callback_requests.CallbackRequest]

   .. attribute:: class_creation_counter
      :annotation: = 0

      

   .. attribute:: file_path
      

      

   .. attribute:: pid
      

      :return: the PID of the process launched to process the given file
      :rtype: int


   .. attribute:: exit_code
      

      After the process is finished, this can be called to get the return code

      :return: the exit code of the process
      :rtype: int


   .. attribute:: done
      

      Check if the process launched to process this file is done.

      :return: whether the process is finished running
      :rtype: bool


   .. attribute:: result
      

      :return: result of running SchedulerJob.process_file()
      :rtype: tuple[int, int] or None


   .. attribute:: start_time
      

      :return: when this started to process the file
      :rtype: datetime


   .. attribute:: waitable_handle
      

      

   
   .. staticmethod:: _run_file_processor(result_channel: MultiprocessingConnection, parent_channel: MultiprocessingConnection, file_path: str, pickle_dags: bool, dag_ids: Optional[List[str]], thread_name: str, callback_requests: List[CallbackRequest])

      Process the given file.

      :param result_channel: the connection to use for passing back the result
      :type result_channel: multiprocessing.Connection
      :param parent_channel: the parent end of the channel to close in the child
      :type result_channel: multiprocessing.Connection
      :param file_path: the file to process
      :type file_path: str
      :param pickle_dags: whether to pickle the DAGs found in the file and
          save them to the DB
      :type pickle_dags: bool
      :param dag_ids: if specified, only examine DAG ID's that are
          in this list
      :type dag_ids: list[str]
      :param thread_name: the name to use for the process that is launched
      :type thread_name: str
      :param callback_requests: failure callback to execute
      :type callback_requests: List[airflow.utils.callback_requests.CallbackRequest]
      :return: the process that was launched
      :rtype: multiprocessing.Process



   
   .. method:: start(self)

      Launch the process and start processing the DAG.



   
   .. method:: kill(self)

      Kill the process launched to process the file, and ensure consistent state.



   
   .. method:: terminate(self, sigkill: bool = False)

      Terminate (and then kill) the process launched to process the file.

      :param sigkill: whether to issue a SIGKILL if SIGTERM doesn't work.
      :type sigkill: bool



   
   .. method:: _kill_process(self)




.. py:class:: DagFileProcessor(dag_ids: Optional[List[str]], log: logging.Logger)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Process a Python file containing Airflow DAGs.

   This includes:

   1. Execute the file and look for DAG objects in the namespace.
   2. Pickle the DAG and save it to the DB (if necessary).
   3. For each DAG, see what tasks should run and create appropriate task
   instances in the DB.
   4. Record any errors importing the file into ORM
   5. Kill (in ORM) any task instances belonging to the DAGs that haven't
   issued a heartbeat in a while.

   Returns a list of SimpleDag objects that represent the DAGs found in
   the file

   :param dag_ids: If specified, only look at these DAG ID's
   :type dag_ids: List[str]
   :param log: Logger to save the processing process
   :type log: logging.Logger

   .. attribute:: UNIT_TEST_MODE
      :annotation: :bool

      

   
   .. method:: manage_slas(self, dag: DAG, session: Session = None)

      Finding all tasks that have SLAs defined, and sending alert emails
      where needed. New SLA misses are also recorded in the database.

      We are assuming that the scheduler runs often, so we only check for
      tasks that should have succeeded in the past hour.



   
   .. staticmethod:: update_import_errors(session: Session, dagbag: DagBag)

      For the DAGs in the given DagBag, record any associated import errors and clears
      errors for files that no longer have them. These are usually displayed through the
      Airflow UI so that users know that there are issues parsing DAGs.

      :param session: session for ORM operations
      :type session: sqlalchemy.orm.session.Session
      :param dagbag: DagBag containing DAGs with import errors
      :type dagbag: airflow.DagBag



   
   .. method:: execute_callbacks(self, dagbag: DagBag, callback_requests: List[CallbackRequest], session: Session = None)

      Execute on failure callbacks. These objects can come from SchedulerJob or from
      DagFileProcessorManager.

      :param dagbag: Dag Bag of dags
      :param callback_requests: failure callbacks to execute
      :type callback_requests: List[airflow.utils.callback_requests.CallbackRequest]
      :param session: DB session.



   
   .. method:: _execute_dag_callbacks(self, dagbag: DagBag, request: DagCallbackRequest, session: Session)



   
   .. method:: _execute_task_callbacks(self, dagbag: DagBag, request: TaskCallbackRequest)



   
   .. method:: process_file(self, file_path: str, callback_requests: List[CallbackRequest], pickle_dags: bool = False, session: Session = None)

      Process a Python file containing Airflow DAGs.

      This includes:

      1. Execute the file and look for DAG objects in the namespace.
      2. Pickle the DAG and save it to the DB (if necessary).
      3. For each DAG, see what tasks should run and create appropriate task
      instances in the DB.
      4. Record any errors importing the file into ORM
      5. Kill (in ORM) any task instances belonging to the DAGs that haven't
      issued a heartbeat in a while.

      Returns a list of serialized_dag dicts that represent the DAGs found in
      the file

      :param file_path: the path to the Python file that should be executed
      :type file_path: str
      :param callback_requests: failure callback to execute
      :type callback_requests: List[airflow.utils.dag_processing.CallbackRequest]
      :param pickle_dags: whether serialize the DAGs found in the file and
          save them to the db
      :type pickle_dags: bool
      :param session: Sqlalchemy ORM Session
      :type session: Session
      :return: number of dags found, count of import errors
      :rtype: Tuple[int, int]




.. py:class:: SchedulerJob(subdir: str = settings.DAGS_FOLDER, num_runs: int = conf.getint('scheduler', 'num_runs'), num_times_parse_dags: int = -1, processor_poll_interval: float = conf.getfloat('scheduler', 'processor_poll_interval'), do_pickle: bool = False, log: Any = None, *args, **kwargs)

   Bases: :class:`airflow.jobs.base_job.BaseJob`

   This SchedulerJob runs for a specific time interval and schedules the jobs
   that are ready to run. It figures out the latest runs for each
   task and sees if the dependencies for the next schedules are met.
   If so, it creates appropriate TaskInstances and sends run commands to the
   executor. It does this for each task in each DAG and repeats.

   :param dag_id: if specified, only schedule tasks with this DAG ID
   :type dag_id: str
   :param dag_ids: if specified, only schedule tasks with these DAG IDs
   :type dag_ids: list[str]
   :param subdir: directory containing Python files with Airflow DAG
       definitions, or a specific path to a file
   :type subdir: str
   :param num_runs: The number of times to run the scheduling loop. If you
       have a large number of DAG files this could complete before each file
       has been parsed. -1 for unlimited times.
   :type num_runs: int
   :param num_times_parse_dags: The number of times to try to parse each DAG file.
       -1 for unlimited times.
   :type num_times_parse_dags: int
   :param processor_poll_interval: The number of seconds to wait between
       polls of running processors
   :type processor_poll_interval: int
   :param do_pickle: once a DAG object is obtained by executing the Python
       file, whether to serialize the DAG object to the DB
   :type do_pickle: bool

   .. attribute:: __mapper_args__
      

      

   .. attribute:: heartrate
      :annotation: :int

      

   
   .. method:: register_signals(self)

      Register signals that stop child processes



   
   .. method:: _exit_gracefully(self, signum, frame)

      Helper method to clean up processor_agent to avoid leaving orphan processes.



   
   .. method:: _debug_dump(self, signum, frame)



   
   .. method:: is_alive(self, grace_multiplier: Optional[float] = None)

      Is this SchedulerJob alive?

      We define alive as in a state of running and a heartbeat within the
      threshold defined in the ``scheduler_health_check_threshold`` config
      setting.

      ``grace_multiplier`` is accepted for compatibility with the parent class.

      :rtype: boolean



   
   .. method:: _change_state_for_tis_without_dagrun(self, old_states: List[str], new_state: str, session: Session = None)

      For all DAG IDs in the DagBag, look for task instances in the
      old_states and set them to new_state if the corresponding DagRun
      does not exist or exists but is not in the running state. This
      normally should not happen, but it can if the state of DagRuns are
      changed manually.

      :param old_states: examine TaskInstances in this state
      :type old_states: list[airflow.utils.state.State]
      :param new_state: set TaskInstances to this state
      :type new_state: airflow.utils.state.State



   
   .. method:: __get_concurrency_maps(self, states: List[str], session: Session = None)

      Get the concurrency maps.

      :param states: List of states to query for
      :type states: list[airflow.utils.state.State]
      :return: A map from (dag_id, task_id) to # of task instances and
       a map from (dag_id, task_id) to # of task instances in the given state list
      :rtype: tuple[dict[str, int], dict[tuple[str, str], int]]



   
   .. method:: _executable_task_instances_to_queued(self, max_tis: int, session: Session = None)

      Finds TIs that are ready for execution with respect to pool limits,
      dag concurrency, executor state, and priority.

      :param max_tis: Maximum number of TIs to queue in this loop.
      :type max_tis: int
      :return: list[airflow.models.TaskInstance]



   
   .. method:: _enqueue_task_instances_with_queued_state(self, task_instances: List[TI])

      Takes task_instances, which should have been set to queued, and enqueues them
      with the executor.

      :param task_instances: TaskInstances to enqueue
      :type task_instances: list[TaskInstance]



   
   .. method:: _critical_section_execute_task_instances(self, session: Session)

      Attempts to execute TaskInstances that should be executed by the scheduler.

      There are three steps:
      1. Pick TIs by priority with the constraint that they are in the expected states
      and that we do exceed max_active_runs or pool limits.
      2. Change the state for the TIs above atomically.
      3. Enqueue the TIs in the executor.

      HA note: This function is a "critical section" meaning that only a single executor process can execute
      this function at the same time. This is achieved by doing ``SELECT ... from pool FOR UPDATE``. For DBs
      that support NOWAIT, a "blocked" scheduler will skip this and continue on with other tasks (creating
      new DAG runs, progressing TIs from None to SCHEDULED etc.); DBs that don't support this (such as
      MariaDB or MySQL 5.x) the other schedulers will wait for the lock before continuing.

      :param session:
      :type session: sqlalchemy.orm.Session
      :return: Number of task instance with state changed.



   
   .. method:: _change_state_for_tasks_failed_to_execute(self, session: Session = None)

      If there are tasks left over in the executor,
      we set them back to SCHEDULED to avoid creating hanging tasks.

      :param session: session for ORM operations



   
   .. method:: _process_executor_events(self, session: Session = None)

      Respond to executor events.



   
   .. method:: _execute(self)



   
   .. staticmethod:: _create_dag_file_processor(file_path: str, callback_requests: List[CallbackRequest], dag_ids: Optional[List[str]], pickle_dags: bool)

      Creates DagFileProcessorProcess instance.



   
   .. method:: _run_scheduler_loop(self)

      The actual scheduler loop. The main steps in the loop are:
          #. Harvest DAG parsing results through DagFileProcessorAgent
          #. Find and queue executable tasks
              #. Change task instance state in DB
              #. Queue tasks in executor
          #. Heartbeat executor
              #. Execute queued tasks in executor asynchronously
              #. Sync on the states of running tasks

      Following is a graphic representation of these steps.

      .. image:: ../docs/img/scheduler_loop.jpg

      :rtype: None



   
   .. method:: _clean_tis_without_dagrun(self, session)



   
   .. method:: _do_scheduling(self, session)

      This function is where the main scheduling decisions take places. It:

      - Creates any necessary DAG runs by examining the next_dagrun_create_after column of DagModel

        Since creating Dag Runs is a relatively time consuming process, we select only 10 dags by default
        (configurable via ``scheduler.max_dagruns_to_create_per_loop`` setting) - putting this higher will
        mean one scheduler could spend a chunk of time creating dag runs, and not ever get around to
        scheduling tasks.

      - Finds the "next n oldest" running DAG Runs to examine for scheduling (n=20 by default, configurable
        via ``scheduler.max_dagruns_per_loop_to_schedule`` config setting) and tries to progress state (TIs
        to SCHEDULED, or DagRuns to SUCCESS/FAILURE etc)

        By "next oldest", we mean hasn't been examined/scheduled in the most time.

        The reason we don't select all dagruns at once because the rows are selected with row locks, meaning
        that only one scheduler can "process them", even it it is waiting behind other dags. Increasing this
        limit will allow more throughput for smaller DAGs but will likely slow down throughput for larger
        (>500 tasks.) DAGs

      - Then, via a Critical Section (locking the rows of the Pool model) we queue tasks, and then send them
        to the executor.

        See docs of _critical_section_execute_task_instances for more.

      :return: Number of TIs enqueued in this iteration
      :rtype: int



   
   .. method:: _create_dag_runs(self, dag_models: Iterable[DagModel], session: Session)

      Unconditionally create a DAG run for the given DAG, and update the dag_model's fields to control
      if/when the next DAGRun should be created



   
   .. method:: _update_dag_next_dagruns(self, dag_models: Iterable[DagModel], session: Session)

      Bulk update the next_dagrun and next_dagrun_create_after for all the dags.

      We batch the select queries to get info about all the dags at once



   
   .. method:: _schedule_dag_run(self, dag_run: DagRun, currently_active_runs: Set[datetime.datetime], session: Session)

      Make scheduling decisions about an individual dag run

      ``currently_active_runs`` is passed in so that a batch query can be
      used to ask this for all dag runs in the batch, to avoid an n+1 query.

      :param dag_run: The DagRun to schedule
      :param currently_active_runs: Number of currently active runs of this DAG
      :return: Number of tasks scheduled



   
   .. method:: _verify_integrity_if_dag_changed(self, dag_run: DagRun, session=None)

      Only run DagRun.verify integrity if Serialized DAG has changed since it is slow



   
   .. method:: _send_dag_callbacks_to_processor(self, dag_run: DagRun, callback: Optional[DagCallbackRequest] = None)



   
   .. method:: _send_sla_callbacks_to_processor(self, dag: DAG)

      Sends SLA Callbacks to DagFileProcessor if tasks have SLAs set and check_slas=True



   
   .. method:: _emit_pool_metrics(self, session: Session = None)



   
   .. method:: heartbeat_callback(self, session: Session = None)



   
   .. method:: adopt_or_reset_orphaned_tasks(self, session: Session = None)

      Reset any TaskInstance still in QUEUED or SCHEDULED states that were
      enqueued by a SchedulerJob that is no longer running.

      :return: the number of TIs reset
      :rtype: int




