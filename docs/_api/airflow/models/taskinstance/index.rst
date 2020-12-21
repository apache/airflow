:mod:`airflow.models.taskinstance`
==================================

.. py:module:: airflow.models.taskinstance


Module Contents
---------------

.. data:: ApiClient
   

   

.. data:: TR
   

   

.. data:: Context
   

   

.. data:: _CURRENT_CONTEXT
   :annotation: :List[Context] = []

   

.. data:: log
   

   

.. function:: set_current_context(context: Context)
   Sets the current execution context to the provided context object.
   This method should be called once per Task execution, before calling operator.execute.


.. function:: clear_task_instances(tis, session, activate_dag_runs=True, dag=None)
   Clears a set of task instances, but makes sure the running ones
   get killed.

   :param tis: a list of task instances
   :param session: current session
   :param activate_dag_runs: flag to check for active dag run
   :param dag: DAG object


.. py:class:: TaskInstanceKey

   Bases: :class:`typing.NamedTuple`

   Key used to identify task instance.

   .. attribute:: dag_id
      :annotation: :str

      

   .. attribute:: task_id
      :annotation: :str

      

   .. attribute:: execution_date
      :annotation: :datetime

      

   .. attribute:: try_number
      :annotation: :int

      

   .. attribute:: primary
      

      Return task instance primary key part of the key


   .. attribute:: reduced
      

      Remake the key by subtracting 1 from try number to match in memory information


   
   .. method:: with_try_number(self, try_number: int)

      Returns TaskInstanceKey with provided ``try_number``




.. py:class:: TaskInstance(task, execution_date: datetime, state: Optional[str] = None)

   Bases: :class:`airflow.models.base.Base`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Task instances store the state of a task instance. This table is the
   authority and single source of truth around what tasks have run and the
   state they are in.

   The SqlAlchemy model doesn't have a SqlAlchemy foreign key to the task or
   dag model deliberately to have more control over transactions.

   Database transactions on this table should insure double triggers and
   any confusion around what task instances are or aren't ready to run
   even while multiple schedulers may be firing task instances.

   .. attribute:: __tablename__
      :annotation: = task_instance

      

   .. attribute:: task_id
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: start_date
      

      

   .. attribute:: end_date
      

      

   .. attribute:: duration
      

      

   .. attribute:: state
      

      

   .. attribute:: _try_number
      

      

   .. attribute:: max_tries
      

      

   .. attribute:: hostname
      

      

   .. attribute:: unixname
      

      

   .. attribute:: job_id
      

      

   .. attribute:: pool
      

      

   .. attribute:: pool_slots
      

      

   .. attribute:: queue
      

      

   .. attribute:: priority_weight
      

      

   .. attribute:: operator
      

      

   .. attribute:: queued_dttm
      

      

   .. attribute:: queued_by_job_id
      

      

   .. attribute:: pid
      

      

   .. attribute:: executor_config
      

      

   .. attribute:: external_executor_id
      

      

   .. attribute:: __table_args__
      

      

   .. attribute:: dag_model
      

      

   .. attribute:: try_number
      

      Return the try number that this task number will be when it is actually
      run.

      If the TaskInstance is currently running, this will match the column in the
      database, in all other cases this will be incremented.


   .. attribute:: prev_attempted_tries
      

      Based on this instance's try_number, this will calculate
      the number of previously attempted tries, defaulting to 0.


   .. attribute:: next_try_number
      

      Setting Next Try Number


   .. attribute:: log_filepath
      

      Filepath for TaskInstance


   .. attribute:: log_url
      

      Log URL for TaskInstance


   .. attribute:: mark_success_url
      

      URL to mark TI success


   .. attribute:: key
      

      Returns a tuple that identifies the task instance uniquely


   .. attribute:: is_premature
      

      Returns whether a task is in UP_FOR_RETRY state and its retry interval
      has elapsed.


   .. attribute:: previous_ti
      

      This attribute is deprecated.
      Please use `airflow.models.taskinstance.TaskInstance.get_previous_ti` method.


   .. attribute:: previous_ti_success
      

      This attribute is deprecated.
      Please use `airflow.models.taskinstance.TaskInstance.get_previous_ti` method.


   .. attribute:: previous_start_date_success
      

      This attribute is deprecated.
      Please use `airflow.models.taskinstance.TaskInstance.get_previous_start_date` method.


   
   .. method:: init_on_load(self)

      Initialize the attributes that aren't stored in the DB



   
   .. method:: command_as_list(self, mark_success=False, ignore_all_deps=False, ignore_task_deps=False, ignore_depends_on_past=False, ignore_ti_state=False, local=False, pickle_id=None, raw=False, job_id=None, pool=None, cfg_path=None)

      Returns a command that can be executed anywhere where airflow is
      installed. This command is part of the message sent to executors by
      the orchestrator.



   
   .. staticmethod:: generate_command(dag_id: str, task_id: str, execution_date: datetime, mark_success: bool = False, ignore_all_deps: bool = False, ignore_depends_on_past: bool = False, ignore_task_deps: bool = False, ignore_ti_state: bool = False, local: bool = False, pickle_id: Optional[int] = None, file_path: Optional[str] = None, raw: bool = False, job_id: Optional[str] = None, pool: Optional[str] = None, cfg_path: Optional[str] = None)

      Generates the shell command required to execute this task instance.

      :param dag_id: DAG ID
      :type dag_id: str
      :param task_id: Task ID
      :type task_id: str
      :param execution_date: Execution date for the task
      :type execution_date: datetime
      :param mark_success: Whether to mark the task as successful
      :type mark_success: bool
      :param ignore_all_deps: Ignore all ignorable dependencies.
          Overrides the other ignore_* parameters.
      :type ignore_all_deps: bool
      :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs
          (e.g. for Backfills)
      :type ignore_depends_on_past: bool
      :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past
          and trigger rule
      :type ignore_task_deps: bool
      :param ignore_ti_state: Ignore the task instance's previous failure/success
      :type ignore_ti_state: bool
      :param local: Whether to run the task locally
      :type local: bool
      :param pickle_id: If the DAG was serialized to the DB, the ID
          associated with the pickled DAG
      :type pickle_id: Optional[int]
      :param file_path: path to the file containing the DAG definition
      :type file_path: Optional[str]
      :param raw: raw mode (needs more details)
      :type raw: Optional[bool]
      :param job_id: job ID (needs more details)
      :type job_id: Optional[int]
      :param pool: the Airflow pool that the task should run in
      :type pool: Optional[str]
      :param cfg_path: the Path to the configuration file
      :type cfg_path: Optional[str]
      :return: shell command that can be used to run the task instance
      :rtype: list[str]



   
   .. method:: current_state(self, session=None)

      Get the very latest state from the database, if a session is passed,
      we use and looking up the state becomes part of the session, otherwise
      a new session is used.

      :param session: SQLAlchemy ORM Session
      :type session: Session



   
   .. method:: error(self, session=None)

      Forces the task instance's state to FAILED in the database.

      :param session: SQLAlchemy ORM Session
      :type session: Session



   
   .. method:: refresh_from_db(self, session=None, lock_for_update=False)

      Refreshes the task instance from the database based on the primary key

      :param session: SQLAlchemy ORM Session
      :type session: Session
      :param lock_for_update: if True, indicates that the database should
          lock the TaskInstance (issuing a FOR UPDATE clause) until the
          session is committed.
      :type lock_for_update: bool



   
   .. method:: refresh_from_task(self, task, pool_override=None)

      Copy common attributes from the given task.

      :param task: The task object to copy from
      :type task: airflow.models.BaseOperator
      :param pool_override: Use the pool_override instead of task's pool
      :type pool_override: str



   
   .. method:: clear_xcom_data(self, session=None)

      Clears all XCom data from the database for the task instance

      :param session: SQLAlchemy ORM Session
      :type session: Session



   
   .. method:: set_state(self, state: str, session=None)

      Set TaskInstance state.

      :param state: State to set for the TI
      :type state: str
      :param session: SQLAlchemy ORM Session
      :type session: Session



   
   .. method:: are_dependents_done(self, session=None)

      Checks whether the immediate dependents of this task instance have succeeded or have been skipped.
      This is meant to be used by wait_for_downstream.

      This is useful when you do not want to start processing the next
      schedule of a task until the dependents are done. For instance,
      if the task DROPs and recreates a table.

      :param session: SQLAlchemy ORM Session
      :type session: Session



   
   .. method:: get_previous_ti(self, state: Optional[str] = None, session: Session = None)

      The task instance for the task that ran before this task instance.

      :param state: If passed, it only take into account instances of a specific state.
      :param session: SQLAlchemy ORM Session



   
   .. method:: get_previous_execution_date(self, state: Optional[str] = None, session: Session = None)

      The execution date from property previous_ti_success.

      :param state: If passed, it only take into account instances of a specific state.
      :param session: SQLAlchemy ORM Session



   
   .. method:: get_previous_start_date(self, state: Optional[str] = None, session: Session = None)

      The start date from property previous_ti_success.

      :param state: If passed, it only take into account instances of a specific state.
      :param session: SQLAlchemy ORM Session



   
   .. method:: are_dependencies_met(self, dep_context=None, session=None, verbose=False)

      Returns whether or not all the conditions are met for this task instance to be run
      given the context for the dependencies (e.g. a task instance being force run from
      the UI will ignore some dependencies).

      :param dep_context: The execution context that determines the dependencies that
          should be evaluated.
      :type dep_context: DepContext
      :param session: database session
      :type session: sqlalchemy.orm.session.Session
      :param verbose: whether log details on failed dependencies on
          info or debug log level
      :type verbose: bool



   
   .. method:: get_failed_dep_statuses(self, dep_context=None, session=None)

      Get failed Dependencies



   
   .. method:: __repr__(self)



   
   .. method:: next_retry_datetime(self)

      Get datetime of the next retry if the task instance fails. For exponential
      backoff, retry_delay is used as base and will be converted to seconds.



   
   .. method:: ready_for_retry(self)

      Checks on whether the task instance is in the right state and timeframe
      to be retried.



   
   .. method:: get_dagrun(self, session: Session = None)

      Returns the DagRun for this TaskInstance

      :param session: SQLAlchemy ORM Session
      :return: DagRun



   
   .. method:: check_and_change_state_before_execution(self, verbose: bool = True, ignore_all_deps: bool = False, ignore_depends_on_past: bool = False, ignore_task_deps: bool = False, ignore_ti_state: bool = False, mark_success: bool = False, test_mode: bool = False, job_id: Optional[str] = None, pool: Optional[str] = None, session=None)

      Checks dependencies and then sets state to RUNNING if they are met. Returns
      True if and only if state is set to RUNNING, which implies that task should be
      executed, in preparation for _run_raw_task

      :param verbose: whether to turn on more verbose logging
      :type verbose: bool
      :param ignore_all_deps: Ignore all of the non-critical dependencies, just runs
      :type ignore_all_deps: bool
      :param ignore_depends_on_past: Ignore depends_on_past DAG attribute
      :type ignore_depends_on_past: bool
      :param ignore_task_deps: Don't check the dependencies of this TaskInstance's task
      :type ignore_task_deps: bool
      :param ignore_ti_state: Disregards previous task instance state
      :type ignore_ti_state: bool
      :param mark_success: Don't run the task, mark its state as success
      :type mark_success: bool
      :param test_mode: Doesn't record success or failure in the DB
      :type test_mode: bool
      :param job_id: Job (BackfillJob / LocalTaskJob / SchedulerJob) ID
      :type job_id: str
      :param pool: specifies the pool to use to run the task instance
      :type pool: str
      :param session: SQLAlchemy ORM Session
      :type session: Session
      :return: whether the state was changed to running or not
      :rtype: bool



   
   .. method:: _date_or_empty(self, attr)



   
   .. method:: _run_raw_task(self, mark_success: bool = False, test_mode: bool = False, job_id: Optional[str] = None, pool: Optional[str] = None, session=None)

      Immediately runs the task (without checking or changing db state
      before execution) and then sets the appropriate final state after
      completion and runs any post-execute callbacks. Meant to be called
      only after another function changes the state to running.

      :param mark_success: Don't run the task, mark its state as success
      :type mark_success: bool
      :param test_mode: Doesn't record success or failure in the DB
      :type test_mode: bool
      :param pool: specifies the pool to use to run the task instance
      :type pool: str
      :param session: SQLAlchemy ORM Session
      :type session: Session



   
   .. method:: _run_mini_scheduler_on_child_tasks(self, session=None)



   
   .. method:: _prepare_and_execute_task_with_callbacks(self, context, task)

      Prepare Task for Execution



   
   .. method:: _update_ti_state_for_sensing(self, session=None)



   
   .. method:: _run_success_callback(self, context, task)

      Functions that need to be run if Task is successful



   
   .. method:: _execute_task(self, context, task_copy)

      Executes Task (optionally with a Timeout) and pushes Xcom results



   
   .. method:: _run_execute_callback(self, context, task)

      Functions that need to be run before a Task is executed



   
   .. method:: run(self, verbose: bool = True, ignore_all_deps: bool = False, ignore_depends_on_past: bool = False, ignore_task_deps: bool = False, ignore_ti_state: bool = False, mark_success: bool = False, test_mode: bool = False, job_id: Optional[str] = None, pool: Optional[str] = None, session=None)

      Run TaskInstance



   
   .. method:: dry_run(self)

      Only Renders Templates for the TI



   
   .. method:: _handle_reschedule(self, actual_start_date, reschedule_exception, test_mode=False, session=None)



   
   .. method:: handle_failure(self, error, test_mode=None, context=None, force_fail=False, session=None)

      Handle Failure for the TaskInstance



   
   .. method:: is_eligible_to_retry(self)

      Is task instance is eligible for retry



   
   .. method:: _safe_date(self, date_attr, fmt)



   
   .. method:: get_template_context(self, session=None)

      Return TI Context



   
   .. method:: get_rendered_template_fields(self)

      Fetch rendered template fields from DB



   
   .. method:: get_rendered_k8s_spec(self)

      Fetch rendered template fields from DB



   
   .. method:: overwrite_params_with_dag_run_conf(self, params, dag_run)

      Overwrite Task Params with DagRun.conf



   
   .. method:: render_templates(self, context: Optional[Context] = None)

      Render templates in the operator fields.



   
   .. method:: render_k8s_pod_yaml(self)

      Render k8s pod yaml



   
   .. method:: get_email_subject_content(self, exception)

      Get the email subject content for exceptions.



   
   .. method:: email_alert(self, exception)

      Send alert email with exception information.



   
   .. method:: set_duration(self)

      Set TI duration



   
   .. method:: xcom_push(self, key: str, value: Any, execution_date: Optional[datetime] = None, session: Session = None)

      Make an XCom available for tasks to pull.

      :param key: A key for the XCom
      :type key: str
      :param value: A value for the XCom. The value is pickled and stored
          in the database.
      :type value: any picklable object
      :param execution_date: if provided, the XCom will not be visible until
          this date. This can be used, for example, to send a message to a
          task on a future date without it being immediately visible.
      :type execution_date: datetime
      :param session: Sqlalchemy ORM Session
      :type session: Session



   
   .. method:: xcom_pull(self, task_ids: Optional[Union[str, Iterable[str]]] = None, dag_id: Optional[str] = None, key: str = XCOM_RETURN_KEY, include_prior_dates: bool = False, session: Session = None)

      Pull XComs that optionally meet certain criteria.

      The default value for `key` limits the search to XComs
      that were returned by other tasks (as opposed to those that were pushed
      manually). To remove this filter, pass key=None (or any desired value).

      If a single task_id string is provided, the result is the value of the
      most recent matching XCom from that task_id. If multiple task_ids are
      provided, a tuple of matching values is returned. None is returned
      whenever no matches are found.

      :param key: A key for the XCom. If provided, only XComs with matching
          keys will be returned. The default key is 'return_value', also
          available as a constant XCOM_RETURN_KEY. This key is automatically
          given to XComs returned by tasks (as opposed to being pushed
          manually). To remove the filter, pass key=None.
      :type key: str
      :param task_ids: Only XComs from tasks with matching ids will be
          pulled. Can pass None to remove the filter.
      :type task_ids: str or iterable of strings (representing task_ids)
      :param dag_id: If provided, only pulls XComs from this DAG.
          If None (default), the DAG of the calling task is used.
      :type dag_id: str
      :param include_prior_dates: If False, only XComs from the current
          execution_date are returned. If True, XComs from previous dates
          are returned as well.
      :type include_prior_dates: bool
      :param session: Sqlalchemy ORM Session
      :type session: Session



   
   .. method:: get_num_running_task_instances(self, session)

      Return Number of running TIs from the DB



   
   .. method:: init_run_context(self, raw=False)

      Sets the log context.



   
   .. staticmethod:: filter_for_tis(tis: Iterable[Union['TaskInstance', TaskInstanceKey]])

      Returns SQLAlchemy filter to query selected task instances




.. data:: TaskInstanceStateType
   

   

.. py:class:: SimpleTaskInstance(ti: TaskInstance)

   Simplified Task Instance.

   Used to send data between processes via Queues.

   .. attribute:: dag_id
      

      

   .. attribute:: task_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: start_date
      

      

   .. attribute:: end_date
      

      

   .. attribute:: try_number
      

      

   .. attribute:: state
      

      

   .. attribute:: pool
      

      

   .. attribute:: priority_weight
      

      

   .. attribute:: queue
      

      

   .. attribute:: key
      

      

   .. attribute:: executor_config
      

      

   
   .. method:: construct_task_instance(self, session=None, lock_for_update=False)

      Construct a TaskInstance from the database based on the primary key

      :param session: DB session.
      :param lock_for_update: if True, indicates that the database should
          lock the TaskInstance (issuing a FOR UPDATE clause) until the
          session is committed.
      :return: the task instance constructed




.. data:: STATICA_HACK
   :annotation: = True

   

.. data:: dag_run
   

   

