:mod:`airflow.models.dagrun`
============================

.. py:module:: airflow.models.dagrun


Module Contents
---------------

.. py:class:: TISchedulingDecision

   Bases: :class:`typing.NamedTuple`

   Type of return for DagRun.task_instance_scheduling_decisions

   .. attribute:: tis
      :annotation: :List[TI]

      

   .. attribute:: schedulable_tis
      :annotation: :List[TI]

      

   .. attribute:: changed_tis
      :annotation: :bool

      

   .. attribute:: unfinished_tasks
      :annotation: :List[TI]

      

   .. attribute:: finished_tasks
      :annotation: :List[TI]

      


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




