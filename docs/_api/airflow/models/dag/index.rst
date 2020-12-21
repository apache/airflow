:mod:`airflow.models.dag`
=========================

.. py:module:: airflow.models.dag


Module Contents
---------------

.. data:: PatternType
   

   

.. data:: log
   

   

.. data:: ScheduleInterval
   

   

.. data:: DEFAULT_VIEW_PRESETS
   :annotation: = ['tree', 'graph', 'duration', 'gantt', 'landing_times']

   

.. data:: ORIENTATION_PRESETS
   :annotation: = ['LR', 'TB', 'RL', 'BT']

   

.. data:: DagStateChangeCallback
   

   

.. function:: get_last_dagrun(dag_id, session, include_externally_triggered=False)
   Returns the last dag run for a dag, None if there was none.
   Last dag run can be any type of run eg. scheduled or backfilled.
   Overridden DagRuns are ignored.


.. py:class:: DAG(dag_id: str, description: Optional[str] = None, schedule_interval: Optional[ScheduleInterval] = timedelta(days=1), start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, full_filepath: Optional[str] = None, template_searchpath: Optional[Union[str, Iterable[str]]] = None, template_undefined: Type[jinja2.StrictUndefined] = jinja2.StrictUndefined, user_defined_macros: Optional[Dict] = None, user_defined_filters: Optional[Dict] = None, default_args: Optional[Dict] = None, concurrency: int = conf.getint('core', 'dag_concurrency'), max_active_runs: int = conf.getint('core', 'max_active_runs_per_dag'), dagrun_timeout: Optional[timedelta] = None, sla_miss_callback: Optional[Callable] = None, default_view: str = conf.get('webserver', 'dag_default_view').lower(), orientation: str = conf.get('webserver', 'dag_orientation'), catchup: bool = conf.getboolean('scheduler', 'catchup_by_default'), on_success_callback: Optional[DagStateChangeCallback] = None, on_failure_callback: Optional[DagStateChangeCallback] = None, doc_md: Optional[str] = None, params: Optional[Dict] = None, access_control: Optional[Dict] = None, is_paused_upon_creation: Optional[bool] = None, jinja_environment_kwargs: Optional[Dict] = None, tags: Optional[List[str]] = None)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   A dag (directed acyclic graph) is a collection of tasks with directional
   dependencies. A dag also has a schedule, a start date and an end date
   (optional). For each schedule, (say daily or hourly), the DAG needs to run
   each individual tasks as their dependencies are met. Certain tasks have
   the property of depending on their own past, meaning that they can't run
   until their previous schedule (and upstream tasks) are completed.

   DAGs essentially act as namespaces for tasks. A task_id can only be
   added once to a DAG.

   :param dag_id: The id of the DAG; must consist exclusively of alphanumeric
       characters, dashes, dots and underscores (all ASCII)
   :type dag_id: str
   :param description: The description for the DAG to e.g. be shown on the webserver
   :type description: str
   :param schedule_interval: Defines how often that DAG runs, this
       timedelta object gets added to your latest task instance's
       execution_date to figure out the next schedule
   :type schedule_interval: datetime.timedelta or
       dateutil.relativedelta.relativedelta or str that acts as a cron
       expression
   :param start_date: The timestamp from which the scheduler will
       attempt to backfill
   :type start_date: datetime.datetime
   :param end_date: A date beyond which your DAG won't run, leave to None
       for open ended scheduling
   :type end_date: datetime.datetime
   :param template_searchpath: This list of folders (non relative)
       defines where jinja will look for your templates. Order matters.
       Note that jinja/airflow includes the path of your DAG file by
       default
   :type template_searchpath: str or list[str]
   :param template_undefined: Template undefined type.
   :type template_undefined: jinja2.StrictUndefined
   :param user_defined_macros: a dictionary of macros that will be exposed
       in your jinja templates. For example, passing ``dict(foo='bar')``
       to this argument allows you to ``{{ foo }}`` in all jinja
       templates related to this DAG. Note that you can pass any
       type of object here.
   :type user_defined_macros: dict
   :param user_defined_filters: a dictionary of filters that will be exposed
       in your jinja templates. For example, passing
       ``dict(hello=lambda name: 'Hello %s' % name)`` to this argument allows
       you to ``{{ 'world' | hello }}`` in all jinja templates related to
       this DAG.
   :type user_defined_filters: dict
   :param default_args: A dictionary of default parameters to be used
       as constructor keyword parameters when initialising operators.
       Note that operators have the same hook, and precede those defined
       here, meaning that if your dict contains `'depends_on_past': True`
       here and `'depends_on_past': False` in the operator's call
       `default_args`, the actual value will be `False`.
   :type default_args: dict
   :param params: a dictionary of DAG level parameters that are made
       accessible in templates, namespaced under `params`. These
       params can be overridden at the task level.
   :type params: dict
   :param concurrency: the number of task instances allowed to run
       concurrently
   :type concurrency: int
   :param max_active_runs: maximum number of active DAG runs, beyond this
       number of DAG runs in a running state, the scheduler won't create
       new active DAG runs
   :type max_active_runs: int
   :param dagrun_timeout: specify how long a DagRun should be up before
       timing out / failing, so that new DagRuns can be created. The timeout
       is only enforced for scheduled DagRuns.
   :type dagrun_timeout: datetime.timedelta
   :param sla_miss_callback: specify a function to call when reporting SLA
       timeouts.
   :type sla_miss_callback: types.FunctionType
   :param default_view: Specify DAG default view (tree, graph, duration,
                                                  gantt, landing_times), default tree
   :type default_view: str
   :param orientation: Specify DAG orientation in graph view (LR, TB, RL, BT), default LR
   :type orientation: str
   :param catchup: Perform scheduler catchup (or only run latest)? Defaults to True
   :type catchup: bool
   :param on_failure_callback: A function to be called when a DagRun of this dag fails.
       A context dictionary is passed as a single parameter to this function.
   :type on_failure_callback: callable
   :param on_success_callback: Much like the ``on_failure_callback`` except
       that it is executed when the dag succeeds.
   :type on_success_callback: callable
   :param access_control: Specify optional DAG-level permissions, e.g.,
       "{'role1': {'can_read'}, 'role2': {'can_read', 'can_edit'}}"
   :type access_control: dict
   :param is_paused_upon_creation: Specifies if the dag is paused when created for the first time.
       If the dag exists already, this flag will be ignored. If this optional parameter
       is not specified, the global config setting will be used.
   :type is_paused_upon_creation: bool or None
   :param jinja_environment_kwargs: additional configuration options to be passed to Jinja
       ``Environment`` for template rendering

       **Example**: to avoid Jinja from removing a trailing newline from template strings ::

           DAG(dag_id='my-dag',
               jinja_environment_kwargs={
                   'keep_trailing_newline': True,
                   # some other jinja2 Environment options here
               }
           )

       **See**: `Jinja Environment documentation
       <https://jinja.palletsprojects.com/en/master/api/#jinja2.Environment>`_

   :type jinja_environment_kwargs: dict
   :param tags: List of tags to help filtering DAGS in the UI.
   :type tags: List[str]

   .. attribute:: _comps
      

      

   .. attribute:: __serialized_fields
      :annotation: :Optional[FrozenSet[str]]

      

   .. attribute:: dag_id
      

      

   .. attribute:: full_filepath
      

      

   .. attribute:: concurrency
      

      

   .. attribute:: access_control
      

      

   .. attribute:: description
      

      

   .. attribute:: default_view
      

      

   .. attribute:: pickle_id
      

      

   .. attribute:: tasks
      

      

   .. attribute:: task_ids
      

      

   .. attribute:: task_group
      

      

   .. attribute:: filepath
      

      File location of where the dag object is instantiated


   .. attribute:: folder
      

      Folder location of where the DAG object is instantiated.


   .. attribute:: owner
      

      Return list of all owners found in DAG tasks.

      :return: Comma separated list of owners in DAG tasks
      :rtype: str


   .. attribute:: allow_future_exec_dates
      

      

   .. attribute:: concurrency_reached
      

      This attribute is deprecated. Please use `airflow.models.DAG.get_concurrency_reached` method.


   .. attribute:: is_paused
      

      This attribute is deprecated. Please use `airflow.models.DAG.get_is_paused` method.


   .. attribute:: normalized_schedule_interval
      

      Returns Normalized Schedule Interval. This is used internally by the Scheduler to
      schedule DAGs.

      1. Converts Cron Preset to a Cron Expression (e.g ``@monthly`` to ``0 0 1 * *``)
      2. If Schedule Interval is "@once" return "None"
      3. If not (1) or (2) returns schedule_interval


   .. attribute:: latest_execution_date
      

      This attribute is deprecated. Please use `airflow.models.DAG.get_latest_execution_date` method.


   .. attribute:: subdags
      

      Returns a list of the subdag objects associated to this DAG


   .. attribute:: roots
      

      Return nodes with no parents. These are first to execute and are called roots or root nodes.


   .. attribute:: leaves
      

      Return nodes with no children. These are last to execute and are called leaves or leaf nodes.


   .. attribute:: task
      

      

   
   .. method:: __repr__(self)



   
   .. method:: __eq__(self, other)



   
   .. method:: __ne__(self, other)



   
   .. method:: __lt__(self, other)



   
   .. method:: __hash__(self)



   
   .. method:: __enter__(self)



   
   .. method:: __exit__(self, _type, _value, _tb)



   
   .. staticmethod:: _upgrade_outdated_dag_access_control(access_control=None)

      Looks for outdated dag level permissions (can_dag_read and can_dag_edit) in DAG
      access_controls (for example, {'role1': {'can_dag_read'}, 'role2': {'can_dag_read', 'can_dag_edit'}})
      and replaces them with updated permissions (can_read and can_edit).



   
   .. method:: date_range(self, start_date: datetime, num: Optional[int] = None, end_date: Optional[datetime] = timezone.utcnow())



   
   .. method:: is_fixed_time_schedule(self)

      Figures out if the DAG schedule has a fixed time (e.g. 3 AM).

      :return: True if the schedule has a fixed time, False if not.



   
   .. method:: following_schedule(self, dttm)

      Calculates the following schedule for this dag in UTC.

      :param dttm: utc datetime
      :return: utc datetime



   
   .. method:: previous_schedule(self, dttm)

      Calculates the previous schedule for this dag in UTC

      :param dttm: utc datetime
      :return: utc datetime



   
   .. method:: next_dagrun_info(self, date_last_automated_dagrun: Optional[pendulum.DateTime])

      Get information about the next DagRun of this dag after ``date_last_automated_dagrun`` -- the
      execution date, and the earliest it could be scheduled

      :param date_last_automated_dagrun: The max(execution_date) of existing
          "automated" DagRuns for this dag (scheduled or backfill, but not
          manual)



   
   .. method:: next_dagrun_after_date(self, date_last_automated_dagrun: Optional[pendulum.DateTime])

      Get the next execution date after the given ``date_last_automated_dagrun``, according to
      schedule_interval, start_date, end_date etc.  This doesn't check max active run or any other
      "concurrency" type limits, it only performs calculations based on the various date and interval fields
      of this dag and it's tasks.

      :param date_last_automated_dagrun: The execution_date of the last scheduler or
          backfill triggered run for this dag
      :type date_last_automated_dagrun: pendulum.Pendulum



   
   .. method:: get_run_dates(self, start_date, end_date=None)

      Returns a list of dates between the interval received as parameter using this
      dag's schedule interval. Returned dates can be used for execution dates.

      :param start_date: the start date of the interval
      :type start_date: datetime
      :param end_date: the end date of the interval, defaults to timezone.utcnow()
      :type end_date: datetime
      :return: a list of dates within the interval following the dag's schedule
      :rtype: list



   
   .. method:: normalize_schedule(self, dttm)

      Returns dttm + interval unless dttm is first interval then it returns dttm



   
   .. method:: get_last_dagrun(self, session=None, include_externally_triggered=False)



   
   .. method:: has_dag_runs(self, session=None, include_externally_triggered=True)



   
   .. method:: param(self, name: str, default=None)

      Return a DagParam object for current dag.

      :param name: dag parameter name.
      :param default: fallback value for dag parameter.
      :return: DagParam instance for specified name and current dag.



   
   .. method:: get_concurrency_reached(self, session=None)

      Returns a boolean indicating whether the concurrency limit for this DAG
      has been reached



   
   .. method:: get_is_paused(self, session=None)

      Returns a boolean indicating whether this DAG is paused



   
   .. method:: handle_callback(self, dagrun, success=True, reason=None, session=None)

      Triggers the appropriate callback depending on the value of success, namely the
      on_failure_callback or on_success_callback. This method gets the context of a
      single TaskInstance part of this DagRun and passes that to the callable along
      with a 'reason', primarily to differentiate DagRun failures.

      .. note: The logs end up in
          ``$AIRFLOW_HOME/logs/scheduler/latest/PROJECT/DAG_FILE.py.log``

      :param dagrun: DagRun object
      :param success: Flag to specify if failure or success callback should be called
      :param reason: Completion reason
      :param session: Database session



   
   .. method:: get_active_runs(self)

      Returns a list of dag run execution dates currently running

      :return: List of execution dates



   
   .. method:: get_num_active_runs(self, external_trigger=None, session=None)

      Returns the number of active "running" dag runs

      :param external_trigger: True for externally triggered active dag runs
      :type external_trigger: bool
      :param session:
      :return: number greater than 0 for active dag runs



   
   .. method:: get_dagrun(self, execution_date, session=None)

      Returns the dag run for a given execution date if it exists, otherwise
      none.

      :param execution_date: The execution date of the DagRun to find.
      :param session:
      :return: The DagRun if found, otherwise None.



   
   .. method:: get_dagruns_between(self, start_date, end_date, session=None)

      Returns the list of dag runs between start_date (inclusive) and end_date (inclusive).

      :param start_date: The starting execution date of the DagRun to find.
      :param end_date: The ending execution date of the DagRun to find.
      :param session:
      :return: The list of DagRuns found.



   
   .. method:: get_latest_execution_date(self, session=None)

      Returns the latest date for which at least one dag run exists



   
   .. method:: resolve_template_files(self)



   
   .. method:: get_template_env(self)

      Build a Jinja2 environment.



   
   .. method:: set_dependency(self, upstream_task_id, downstream_task_id)

      Simple utility method to set dependency between two tasks that
      already have been added to the DAG using add_task()



   
   .. method:: get_task_instances(self, start_date=None, end_date=None, state=None, session=None)



   
   .. method:: topological_sort(self, include_subdag_tasks: bool = False)

      Sorts tasks in topographical order, such that a task comes after any of its
      upstream dependencies.

      Heavily inspired by:
      http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/

      :param include_subdag_tasks: whether to include tasks in subdags, default to False
      :return: list of tasks in topological order



   
   .. method:: set_dag_runs_state(self, state: str = State.RUNNING, session: Session = None, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None)



   
   .. method:: clear(self, start_date=None, end_date=None, only_failed=False, only_running=False, confirm_prompt=False, include_subdags=True, include_parentdag=True, dag_run_state: str = State.RUNNING, dry_run=False, session=None, get_tis=False, recursion_depth=0, max_recursion_depth=None, dag_bag=None, visited_external_tis=None)

      Clears a set of task instances associated with the current dag for
      a specified date range.

      :param start_date: The minimum execution_date to clear
      :type start_date: datetime.datetime or None
      :param end_date: The maximum execution_date to clear
      :type end_date: datetime.datetime or None
      :param only_failed: Only clear failed tasks
      :type only_failed: bool
      :param only_running: Only clear running tasks.
      :type only_running: bool
      :param confirm_prompt: Ask for confirmation
      :type confirm_prompt: bool
      :param include_subdags: Clear tasks in subdags and clear external tasks
          indicated by ExternalTaskMarker
      :type include_subdags: bool
      :param include_parentdag: Clear tasks in the parent dag of the subdag.
      :type include_parentdag: bool
      :param dag_run_state: state to set DagRun to
      :param dry_run: Find the tasks to clear but don't clear them.
      :type dry_run: bool
      :param session: The sqlalchemy session to use
      :type session: sqlalchemy.orm.session.Session
      :param get_tis: Return the sqlalchemy query for finding the TaskInstance without clearing the tasks
      :type get_tis: bool
      :param recursion_depth: The recursion depth of nested calls to DAG.clear().
      :type recursion_depth: int
      :param max_recursion_depth: The maximum recursion depth allowed. This is determined by the
          first encountered ExternalTaskMarker. Default is None indicating no ExternalTaskMarker
          has been encountered.
      :type max_recursion_depth: int
      :param dag_bag: The DagBag used to find the dags
      :type dag_bag: airflow.models.dagbag.DagBag
      :param visited_external_tis: A set used internally to keep track of the visited TaskInstance when
          clearing tasks across multiple DAGs linked by ExternalTaskMarker to avoid redundant work.
      :type visited_external_tis: set



   
   .. classmethod:: clear_dags(cls, dags, start_date=None, end_date=None, only_failed=False, only_running=False, confirm_prompt=False, include_subdags=True, include_parentdag=False, dag_run_state=State.RUNNING, dry_run=False)



   
   .. method:: __deepcopy__(self, memo)



   
   .. method:: sub_dag(self, *args, **kwargs)

      This method is deprecated in favor of partial_subset



   
   .. method:: partial_subset(self, task_ids_or_regex: Union[str, PatternType, Iterable[str]], include_downstream=False, include_upstream=True, include_direct_upstream=False)

      Returns a subset of the current dag as a deep copy of the current dag
      based on a regex that should match one or many tasks, and includes
      upstream and downstream neighbours based on the flag passed.

      :param task_ids_or_regex: Either a list of task_ids, or a regex to
          match against task ids (as a string, or compiled regex pattern).
      :type task_ids_or_regex: [str] or str or re.Pattern
      :param include_downstream: Include all downstream tasks of matched
          tasks, in addition to matched tasks.
      :param include_upstream: Include all upstream tasks of matched tasks,
          in addition to matched tasks.



   
   .. method:: has_task(self, task_id: str)



   
   .. method:: get_task(self, task_id: str, include_subdags: bool = False)



   
   .. method:: pickle_info(self)



   
   .. method:: pickle(self, session=None)



   
   .. method:: tree_view(self)

      Print an ASCII tree representation of the DAG.



   
   .. method:: add_task(self, task)

      Add a task to the DAG

      :param task: the task you want to add
      :type task: task



   
   .. method:: add_tasks(self, tasks)

      Add a list of tasks to the DAG

      :param tasks: a lit of tasks you want to add
      :type tasks: list of tasks



   
   .. method:: run(self, start_date=None, end_date=None, mark_success=False, local=False, executor=None, donot_pickle=conf.getboolean('core', 'donot_pickle'), ignore_task_deps=False, ignore_first_depends_on_past=True, pool=None, delay_on_limit_secs=1.0, verbose=False, conf=None, rerun_failed_tasks=False, run_backwards=False)

      Runs the DAG.

      :param start_date: the start date of the range to run
      :type start_date: datetime.datetime
      :param end_date: the end date of the range to run
      :type end_date: datetime.datetime
      :param mark_success: True to mark jobs as succeeded without running them
      :type mark_success: bool
      :param local: True to run the tasks using the LocalExecutor
      :type local: bool
      :param executor: The executor instance to run the tasks
      :type executor: airflow.executor.base_executor.BaseExecutor
      :param donot_pickle: True to avoid pickling DAG object and send to workers
      :type donot_pickle: bool
      :param ignore_task_deps: True to skip upstream tasks
      :type ignore_task_deps: bool
      :param ignore_first_depends_on_past: True to ignore depends_on_past
          dependencies for the first set of tasks only
      :type ignore_first_depends_on_past: bool
      :param pool: Resource pool to use
      :type pool: str
      :param delay_on_limit_secs: Time in seconds to wait before next attempt to run
          dag run when max_active_runs limit has been reached
      :type delay_on_limit_secs: float
      :param verbose: Make logging output more verbose
      :type verbose: bool
      :param conf: user defined dictionary passed from CLI
      :type conf: dict
      :param rerun_failed_tasks:
      :type: bool
      :param run_backwards:
      :type: bool



   
   .. method:: cli(self)

      Exposes a CLI specific to this DAG



   
   .. method:: create_dagrun(self, state, execution_date=None, run_id=None, start_date=None, external_trigger=False, conf=None, run_type=None, session=None, dag_hash=None, creating_job_id=None)

      Creates a dag run from this dag including the tasks associated with this dag.
      Returns the dag run.

      :param run_id: defines the run id for this dag run
      :type run_id: str
      :param run_type: type of DagRun
      :type run_type: airflow.utils.types.DagRunType
      :param execution_date: the execution date of this dag run
      :type execution_date: datetime.datetime
      :param state: the state of the dag run
      :type state: airflow.utils.state.State
      :param start_date: the date this dag run should be evaluated
      :type start_date: datetime
      :param external_trigger: whether this dag run is externally triggered
      :type external_trigger: bool
      :param conf: Dict containing configuration/parameters to pass to the DAG
      :type conf: dict
      :param creating_job_id: id of the job creating this DagRun
      :type creating_job_id: int
      :param session: database session
      :type session: sqlalchemy.orm.session.Session
      :param dag_hash: Hash of Serialized DAG
      :type dag_hash: str



   
   .. classmethod:: bulk_sync_to_db(cls, dags: Collection['DAG'], session=None)

      This method is deprecated in favor of bulk_write_to_db



   
   .. classmethod:: bulk_write_to_db(cls, dags: Collection['DAG'], session=None)

      Ensure the DagModel rows for the given dags are up-to-date in the dag table in the DB, including
      calculated fields.

      Note that this method can be called for both DAGs and SubDAGs. A SubDag is actually a SubDagOperator.

      :param dags: the DAG objects to save to the DB
      :type dags: List[airflow.models.dag.DAG]
      :return: None



   
   .. method:: sync_to_db(self, session=None)

      Save attributes about this DAG to the DB. Note that this method
      can be called for both DAGs and SubDAGs. A SubDag is actually a
      SubDagOperator.

      :return: None



   
   .. method:: get_default_view(self)

      This is only there for backward compatible jinja2 templates



   
   .. staticmethod:: deactivate_unknown_dags(active_dag_ids, session=None)

      Given a list of known DAGs, deactivate any other DAGs that are
      marked as active in the ORM

      :param active_dag_ids: list of DAG IDs that are active
      :type active_dag_ids: list[unicode]
      :return: None



   
   .. staticmethod:: deactivate_stale_dags(expiration_date, session=None)

      Deactivate any DAGs that were last touched by the scheduler before
      the expiration date. These DAGs were likely deleted.

      :param expiration_date: set inactive DAGs that were touched before this
          time
      :type expiration_date: datetime
      :return: None



   
   .. staticmethod:: get_num_task_instances(dag_id, task_ids=None, states=None, session=None)

      Returns the number of task instances in the given DAG.

      :param session: ORM session
      :param dag_id: ID of the DAG to get the task concurrency of
      :type dag_id: unicode
      :param task_ids: A list of valid task IDs for the given DAG
      :type task_ids: list[unicode]
      :param states: A list of states to filter by if supplied
      :type states: list[state]
      :return: The number of running tasks
      :rtype: int



   
   .. classmethod:: get_serialized_fields(cls)

      Stringified DAGs and operators contain exactly these fields.




.. py:class:: DagTag

   Bases: :class:`airflow.models.base.Base`

   A tag name per dag, to allow quick filtering in the DAG view.

   .. attribute:: __tablename__
      :annotation: = dag_tag

      

   .. attribute:: name
      

      

   .. attribute:: dag_id
      

      

   
   .. method:: __repr__(self)




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




.. function:: dag(*dag_args, **dag_kwargs)
   Python dag decorator. Wraps a function into an Airflow DAG.
   Accepts kwargs for operator kwarg. Can be used to parametrize DAGs.

   :param dag_args: Arguments for DAG object
   :type dag_args: list
   :param dag_kwargs: Kwargs for DAG object.
   :type dag_kwargs: dict


.. data:: STATICA_HACK
   :annotation: = True

   

.. data:: serialized_dag
   

   

.. py:class:: DagContext

   DAG context is used to keep the current DAG when DAG is used as ContextManager.

   You can use DAG as context:

   .. code-block:: python

       with DAG(
           dag_id='example_dag',
           default_args=default_args,
           schedule_interval='0 0 * * *',
           dagrun_timeout=timedelta(minutes=60)
       ) as dag:

   If you do this the context stores the DAG and whenever new task is created, it will use
   such stored DAG as the parent DAG.

   .. attribute:: _context_managed_dag
      :annotation: :Optional[DAG]

      

   .. attribute:: _previous_context_managed_dags
      :annotation: :List[DAG] = []

      

   
   .. classmethod:: push_context_managed_dag(cls, dag: DAG)



   
   .. classmethod:: pop_context_managed_dag(cls)



   
   .. classmethod:: get_current_dag(cls)




