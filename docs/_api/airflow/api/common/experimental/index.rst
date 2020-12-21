:mod:`airflow.api.common.experimental`
======================================

.. py:module:: airflow.api.common.experimental

.. autoapi-nested-parse::

   Experimental APIs.



Submodules
----------
.. toctree::
   :titlesonly:
   :maxdepth: 1

   delete_dag/index.rst
   get_code/index.rst
   get_dag_run_state/index.rst
   get_dag_runs/index.rst
   get_lineage/index.rst
   get_task/index.rst
   get_task_instance/index.rst
   mark_tasks/index.rst
   pool/index.rst
   trigger_dag/index.rst


Package Contents
----------------

.. py:exception:: DagNotFound

   Bases: :class:`airflow.exceptions.AirflowNotFoundException`

   Raise when a DAG is not available in the system


.. py:exception:: DagRunNotFound

   Bases: :class:`airflow.exceptions.AirflowNotFoundException`

   Raise when a DAG Run is not available in the system


.. py:exception:: TaskNotFound

   Bases: :class:`airflow.exceptions.AirflowNotFoundException`

   Raise when a Task is not available in the system


.. py:class:: DagBag(dag_folder: Optional[str] = None, include_examples: bool = conf.getboolean('core', 'LOAD_EXAMPLES'), include_smart_sensor: bool = conf.getboolean('smart_sensor', 'USE_SMART_SENSOR'), safe_mode: bool = conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE'), read_dags_from_db: bool = False, store_serialized_dags: Optional[bool] = None)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   A dagbag is a collection of dags, parsed out of a folder tree and has high
   level configuration settings, like what database to use as a backend and
   what executor to use to fire off tasks. This makes it easier to run
   distinct environments for say production and development, tests, or for
   different teams or security profiles. What would have been system level
   settings are now dagbag level so that one system can run multiple,
   independent settings sets.

   :param dag_folder: the folder to scan to find DAGs
   :type dag_folder: unicode
   :param include_examples: whether to include the examples that ship
       with airflow or not
   :type include_examples: bool
   :param include_smart_sensor: whether to include the smart sensor native
       DAGs that create the smart sensor operators for whole cluster
   :type include_smart_sensor: bool
   :param read_dags_from_db: Read DAGs from DB if ``True`` is passed.
       If ``False`` DAGs are read from python files.
   :type read_dags_from_db: bool

   .. attribute:: DAGBAG_IMPORT_TIMEOUT
      

      

   .. attribute:: SCHEDULER_ZOMBIE_TASK_THRESHOLD
      

      

   .. attribute:: store_serialized_dags
      

      Whether or not to read dags from DB


   .. attribute:: dag_ids
      

      :return: a list of DAG IDs in this bag
      :rtype: List[unicode]


   
   .. method:: size(self)

      :return: the amount of dags contained in this dagbag



   
   .. method:: get_dag(self, dag_id, session: Session = None)

      Gets the DAG out of the dictionary, and refreshes it if expired

      :param dag_id: DAG Id
      :type dag_id: str



   
   .. method:: _add_dag_from_db(self, dag_id: str, session: Session)

      Add DAG to DagBag from DB



   
   .. method:: process_file(self, filepath, only_if_updated=True, safe_mode=True)

      Given a path to a python module or zip file, this method imports
      the module and look for dag objects within it.



   
   .. method:: _load_modules_from_file(self, filepath, safe_mode)



   
   .. method:: _load_modules_from_zip(self, filepath, safe_mode)



   
   .. method:: _process_modules(self, filepath, mods, file_last_changed_on_disk)



   
   .. method:: bag_dag(self, dag, root_dag)

      Adds the DAG into the bag, recurses into sub dags.
      Throws AirflowDagCycleException if a cycle is detected in this dag or its subdags



   
   .. method:: collect_dags(self, dag_folder=None, only_if_updated=True, include_examples=conf.getboolean('core', 'LOAD_EXAMPLES'), include_smart_sensor=conf.getboolean('smart_sensor', 'USE_SMART_SENSOR'), safe_mode=conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE'))

      Given a file path or a folder, this method looks for python modules,
      imports them and adds them to the dagbag collection.

      Note that if a ``.airflowignore`` file is found while processing
      the directory, it will behave much like a ``.gitignore``,
      ignoring files that match any of the regex patterns specified
      in the file.

      **Note**: The patterns in .airflowignore are treated as
      un-anchored regexes, not shell-like glob patterns.



   
   .. method:: collect_dags_from_db(self)

      Collects DAGs from database.



   
   .. method:: dagbag_report(self)

      Prints a report around DagBag loading stats



   
   .. method:: sync_to_db(self, session: Optional[Session] = None)

      Save attributes about list of DAG to the DB.




.. py:class:: DagModel(**kwargs)

   Bases: :class:`airflow.models.base.Base`

   Table containing DAG properties

   .. attribute:: __tablename__
      :annotation: = dag

      These items are stored in the database for state related information


   .. attribute:: dag_id
      

      

   .. attribute:: root_dag_id
      

      

   .. attribute:: is_paused_at_creation
      

      

   .. attribute:: is_paused
      

      

   .. attribute:: is_subdag
      

      

   .. attribute:: is_active
      

      

   .. attribute:: last_scheduler_run
      

      

   .. attribute:: last_pickled
      

      

   .. attribute:: last_expired
      

      

   .. attribute:: scheduler_lock
      

      

   .. attribute:: pickle_id
      

      

   .. attribute:: fileloc
      

      

   .. attribute:: owners
      

      

   .. attribute:: description
      

      

   .. attribute:: default_view
      

      

   .. attribute:: schedule_interval
      

      

   .. attribute:: tags
      

      

   .. attribute:: concurrency
      

      

   .. attribute:: has_task_concurrency_limits
      

      

   .. attribute:: next_dagrun
      

      

   .. attribute:: next_dagrun_create_after
      

      

   .. attribute:: __table_args__
      

      

   .. attribute:: NUM_DAGS_PER_DAGRUN_QUERY
      

      

   .. attribute:: timezone
      

      

   .. attribute:: safe_dag_id
      

      

   
   .. method:: __repr__(self)



   
   .. staticmethod:: get_dagmodel(dag_id, session=None)



   
   .. classmethod:: get_current(cls, dag_id, session=None)



   
   .. method:: get_last_dagrun(self, session=None, include_externally_triggered=False)



   
   .. staticmethod:: get_paused_dag_ids(dag_ids: List[str], session: Session = None)

      Given a list of dag_ids, get a set of Paused Dag Ids

      :param dag_ids: List of Dag ids
      :param session: ORM Session
      :return: Paused Dag_ids



   
   .. method:: get_default_view(self)

      Get the Default DAG View, returns the default config value if DagModel does not
      have a value



   
   .. method:: set_is_paused(self, is_paused: bool, including_subdags: bool = True, session=None)

      Pause/Un-pause a DAG.

      :param is_paused: Is the DAG paused
      :param including_subdags: whether to include the DAG's subdags
      :param session: session



   
   .. classmethod:: deactivate_deleted_dags(cls, alive_dag_filelocs: List[str], session=None)

      Set ``is_active=False`` on the DAGs for which the DAG files have been removed.
      Additionally change ``is_active=False`` to ``True`` if the DAG file exists.

      :param alive_dag_filelocs: file paths of alive DAGs
      :param session: ORM Session



   
   .. classmethod:: dags_needing_dagruns(cls, session: Session)

      Return (and lock) a list of Dag objects that are due to create a new DagRun.

      This will return a resultset of rows  that is row-level-locked with a "SELECT ... FOR UPDATE" query,
      you should ensure that any scheduling decisions are made in a single transaction -- as soon as the
      transaction is committed it will be unlocked.



   
   .. method:: calculate_dagrun_date_fields(self, dag: DAG, most_recent_dag_run: Optional[pendulum.DateTime], active_runs_of_dag: int)

      Calculate ``next_dagrun`` and `next_dagrun_create_after``

      :param dag: The DAG object
      :param most_recent_dag_run: DateTime of most recent run of this dag, or none if not yet scheduled.
      :param active_runs_of_dag: Number of currently active runs of this dag




.. py:class:: DagRun(dag_id: Optional[str] = None, run_id: Optional[str] = None, execution_date: Optional[datetime] = None, start_date: Optional[datetime] = None, external_trigger: Optional[bool] = None, conf: Optional[Any] = None, state: Optional[str] = None, run_type: Optional[str] = None, dag_hash: Optional[str] = None, creating_job_id: Optional[int] = None)

   Bases: :class:`airflow.models.base.Base`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   DagRun describes an instance of a Dag. It can be created
   by the scheduler (for regular runs) or by an external trigger

   .. attribute:: __tablename__
      :annotation: = dag_run

      

   .. attribute:: id
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: start_date
      

      

   .. attribute:: end_date
      

      

   .. attribute:: _state
      

      

   .. attribute:: run_id
      

      

   .. attribute:: creating_job_id
      

      

   .. attribute:: external_trigger
      

      

   .. attribute:: run_type
      

      

   .. attribute:: conf
      

      

   .. attribute:: last_scheduling_decision
      

      

   .. attribute:: dag_hash
      

      

   .. attribute:: dag
      

      

   .. attribute:: __table_args__
      

      

   .. attribute:: task_instances
      

      

   .. attribute:: DEFAULT_DAGRUNS_TO_EXAMINE
      

      

   .. attribute:: state
      

      

   .. attribute:: is_backfill
      

      

   
   .. method:: __repr__(self)



   
   .. method:: get_state(self)



   
   .. method:: set_state(self, state)



   
   .. method:: refresh_from_db(self, session: Session = None)

      Reloads the current dagrun from the database

      :param session: database session
      :type session: Session



   
   .. classmethod:: next_dagruns_to_examine(cls, session: Session, max_number: Optional[int] = None)

      Return the next DagRuns that the scheduler should attempt to schedule.

      This will return zero or more DagRun rows that are row-level-locked with a "SELECT ... FOR UPDATE"
      query, you should ensure that any scheduling decisions are made in a single transaction -- as soon as
      the transaction is committed it will be unlocked.

      :rtype: list[airflow.models.DagRun]



   
   .. staticmethod:: find(dag_id: Optional[Union[str, List[str]]] = None, run_id: Optional[str] = None, execution_date: Optional[datetime] = None, state: Optional[str] = None, external_trigger: Optional[bool] = None, no_backfills: bool = False, run_type: Optional[DagRunType] = None, session: Session = None, execution_start_date: Optional[datetime] = None, execution_end_date: Optional[datetime] = None)

      Returns a set of dag runs for the given search criteria.

      :param dag_id: the dag_id or list of dag_id to find dag runs for
      :type dag_id: str or list[str]
      :param run_id: defines the run id for this dag run
      :type run_id: str
      :param run_type: type of DagRun
      :type run_type: airflow.utils.types.DagRunType
      :param execution_date: the execution date
      :type execution_date: datetime.datetime or list[datetime.datetime]
      :param state: the state of the dag run
      :type state: str
      :param external_trigger: whether this dag run is externally triggered
      :type external_trigger: bool
      :param no_backfills: return no backfills (True), return all (False).
          Defaults to False
      :type no_backfills: bool
      :param session: database session
      :type session: sqlalchemy.orm.session.Session
      :param execution_start_date: dag run that was executed from this date
      :type execution_start_date: datetime.datetime
      :param execution_end_date: dag run that was executed until this date
      :type execution_end_date: datetime.datetime



   
   .. staticmethod:: generate_run_id(run_type: DagRunType, execution_date: datetime)

      Generate Run ID based on Run Type and Execution Date



   
   .. method:: get_task_instances(self, state=None, session=None)

      Returns the task instances for this dag run



   
   .. method:: get_task_instance(self, task_id: str, session: Session = None)

      Returns the task instance specified by task_id for this dag run

      :param task_id: the task id
      :type task_id: str
      :param session: Sqlalchemy ORM Session
      :type session: Session



   
   .. method:: get_dag(self)

      Returns the Dag associated with this DagRun.

      :return: DAG



   
   .. method:: get_previous_dagrun(self, state: Optional[str] = None, session: Session = None)

      The previous DagRun, if there is one



   
   .. method:: get_previous_scheduled_dagrun(self, session: Session = None)

      The previous, SCHEDULED DagRun, if there is one



   
   .. method:: update_state(self, session: Session = None, execute_callbacks: bool = True)

      Determines the overall state of the DagRun based on the state
      of its TaskInstances.

      :param session: Sqlalchemy ORM Session
      :type session: Session
      :param execute_callbacks: Should dag callbacks (success/failure, SLA etc) be invoked
          directly (default: true) or recorded as a pending request in the ``callback`` property
      :type execute_callbacks: bool
      :return: Tuple containing tis that can be scheduled in the current loop & `callback` that
          needs to be executed



   
   .. method:: task_instance_scheduling_decisions(self, session: Session = None)



   
   .. method:: _get_ready_tis(self, scheduleable_tasks: List[TI], finished_tasks: List[TI], session: Session)



   
   .. method:: _are_premature_tis(self, unfinished_tasks: List[TI], finished_tasks: List[TI], session: Session)



   
   .. method:: _emit_true_scheduling_delay_stats_for_finished_state(self, finished_tis)

      This is a helper method to emit the true scheduling delay stats, which is defined as
      the time when the first task in DAG starts minus the expected DAG run datetime.
      This method will be used in the update_state method when the state of the DagRun
      is updated to a completed status (either success or failure). The method will find the first
      started task within the DAG and calculate the expected DagRun start time (based on
      dag.execution_date & dag.schedule_interval), and minus these two values to get the delay.
      The emitted data may contains outlier (e.g. when the first task was cleared, so
      the second task's start_date will be used), but we can get rid of the the outliers
      on the stats side through the dashboards tooling built.
      Note, the stat will only be emitted if the DagRun is a scheduler triggered one
      (i.e. external_trigger is False).



   
   .. method:: _emit_duration_stats_for_finished_state(self)



   
   .. method:: verify_integrity(self, session: Session = None)

      Verifies the DagRun by checking for removed tasks or tasks that are not in the
      database yet. It will set state to removed or add the task if required.

      :param session: Sqlalchemy ORM Session
      :type session: Session



   
   .. staticmethod:: get_run(session: Session, dag_id: str, execution_date: datetime)

      Get a single DAG Run

      :param session: Sqlalchemy ORM Session
      :type session: Session
      :param dag_id: DAG ID
      :type dag_id: unicode
      :param execution_date: execution date
      :type execution_date: datetime
      :return: DagRun corresponding to the given dag_id and execution date
          if one exists. None otherwise.
      :rtype: airflow.models.DagRun



   
   .. classmethod:: get_latest_runs(cls, session=None)

      Returns the latest DagRun for each DAG



   
   .. method:: schedule_tis(self, schedulable_tis: Iterable[TI], session: Session = None)

      Set the given task instances in to the scheduled state.

      Each element of ``schedulable_tis`` should have it's ``task`` attribute already set.

      Any DummyOperator without callbacks is instead set straight to the success state.

      All the TIs should belong to this DagRun, but this code is in the hot-path, this is not checked -- it
      is the caller's responsibility to call this function only with TIs from a single dag run.




.. function:: check_and_get_dag(dag_id: str, task_id: Optional[str] = None) -> DagModel
   Checks that DAG exists and in case it is specified that Task exist


.. function:: check_and_get_dagrun(dag: DagModel, execution_date: datetime) -> DagRun
   Get DagRun object and check that it exists


