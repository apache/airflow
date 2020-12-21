:mod:`airflow.models`
=====================

.. py:module:: airflow.models

.. autoapi-nested-parse::

   Airflow models



Submodules
----------
.. toctree::
   :titlesonly:
   :maxdepth: 1

   base/index.rst
   baseoperator/index.rst
   connection/index.rst
   crypto/index.rst
   dag/index.rst
   dagbag/index.rst
   dagcode/index.rst
   dagparam/index.rst
   dagpickle/index.rst
   dagrun/index.rst
   errors/index.rst
   log/index.rst
   pool/index.rst
   renderedtifields/index.rst
   sensorinstance/index.rst
   serialized_dag/index.rst
   skipmixin/index.rst
   slamiss/index.rst
   taskfail/index.rst
   taskinstance/index.rst
   taskmixin/index.rst
   taskreschedule/index.rst
   variable/index.rst
   xcom/index.rst
   xcom_arg/index.rst


Package Contents
----------------

.. data:: ID_LEN
   :annotation: = 250

   

.. data:: Base
   :annotation: :Any

   

.. py:class:: BaseOperator(task_id: str, owner: str = conf.get('operators', 'DEFAULT_OWNER'), email: Optional[Union[str, Iterable[str]]] = None, email_on_retry: bool = conf.getboolean('email', 'default_email_on_retry', fallback=True), email_on_failure: bool = conf.getboolean('email', 'default_email_on_failure', fallback=True), retries: Optional[int] = conf.getint('core', 'default_task_retries', fallback=0), retry_delay: timedelta = timedelta(seconds=300), retry_exponential_backoff: bool = False, max_retry_delay: Optional[datetime] = None, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, depends_on_past: bool = False, wait_for_downstream: bool = False, dag=None, params: Optional[Dict] = None, default_args: Optional[Dict] = None, priority_weight: int = 1, weight_rule: str = WeightRule.DOWNSTREAM, queue: str = conf.get('celery', 'default_queue'), pool: Optional[str] = None, pool_slots: int = 1, sla: Optional[timedelta] = None, execution_timeout: Optional[timedelta] = None, on_execute_callback: Optional[TaskStateChangeCallback] = None, on_failure_callback: Optional[TaskStateChangeCallback] = None, on_success_callback: Optional[TaskStateChangeCallback] = None, on_retry_callback: Optional[TaskStateChangeCallback] = None, trigger_rule: str = TriggerRule.ALL_SUCCESS, resources: Optional[Dict] = None, run_as_user: Optional[str] = None, task_concurrency: Optional[int] = None, executor_config: Optional[Dict] = None, do_xcom_push: bool = True, inlets: Optional[Any] = None, outlets: Optional[Any] = None, task_group: Optional['TaskGroup'] = None, **kwargs)

   Bases: :class:`airflow.models.base.Operator`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`, :class:`airflow.models.taskmixin.TaskMixin`

   Abstract base class for all operators. Since operators create objects that
   become nodes in the dag, BaseOperator contains many recursive methods for
   dag crawling behavior. To derive this class, you are expected to override
   the constructor as well as the 'execute' method.

   Operators derived from this class should perform or trigger certain tasks
   synchronously (wait for completion). Example of operators could be an
   operator that runs a Pig job (PigOperator), a sensor operator that
   waits for a partition to land in Hive (HiveSensorOperator), or one that
   moves data from Hive to MySQL (Hive2MySqlOperator). Instances of these
   operators (tasks) target specific operations, running specific scripts,
   functions or data transfers.

   This class is abstract and shouldn't be instantiated. Instantiating a
   class derived from this one results in the creation of a task object,
   which ultimately becomes a node in DAG objects. Task dependencies should
   be set by using the set_upstream and/or set_downstream methods.

   :param task_id: a unique, meaningful id for the task
   :type task_id: str
   :param owner: the owner of the task, using the unix username is recommended
   :type owner: str
   :param email: the 'to' email address(es) used in email alerts. This can be a
       single email or multiple ones. Multiple addresses can be specified as a
       comma or semi-colon separated string or by passing a list of strings.
   :type email: str or list[str]
   :param email_on_retry: Indicates whether email alerts should be sent when a
       task is retried
   :type email_on_retry: bool
   :param email_on_failure: Indicates whether email alerts should be sent when
       a task failed
   :type email_on_failure: bool
   :param retries: the number of retries that should be performed before
       failing the task
   :type retries: int
   :param retry_delay: delay between retries
   :type retry_delay: datetime.timedelta
   :param retry_exponential_backoff: allow progressive longer waits between
       retries by using exponential backoff algorithm on retry delay (delay
       will be converted into seconds)
   :type retry_exponential_backoff: bool
   :param max_retry_delay: maximum delay interval between retries
   :type max_retry_delay: datetime.timedelta
   :param start_date: The ``start_date`` for the task, determines
       the ``execution_date`` for the first task instance. The best practice
       is to have the start_date rounded
       to your DAG's ``schedule_interval``. Daily jobs have their start_date
       some day at 00:00:00, hourly jobs have their start_date at 00:00
       of a specific hour. Note that Airflow simply looks at the latest
       ``execution_date`` and adds the ``schedule_interval`` to determine
       the next ``execution_date``. It is also very important
       to note that different tasks' dependencies
       need to line up in time. If task A depends on task B and their
       start_date are offset in a way that their execution_date don't line
       up, A's dependencies will never be met. If you are looking to delay
       a task, for example running a daily task at 2AM, look into the
       ``TimeSensor`` and ``TimeDeltaSensor``. We advise against using
       dynamic ``start_date`` and recommend using fixed ones. Read the
       FAQ entry about start_date for more information.
   :type start_date: datetime.datetime
   :param end_date: if specified, the scheduler won't go beyond this date
   :type end_date: datetime.datetime
   :param depends_on_past: when set to true, task instances will run
       sequentially and only if the previous instance has succeeded or has been skipped.
       The task instance for the start_date is allowed to run.
   :type depends_on_past: bool
   :param wait_for_downstream: when set to true, an instance of task
       X will wait for tasks immediately downstream of the previous instance
       of task X to finish successfully or be skipped before it runs. This is useful if the
       different instances of a task X alter the same asset, and this asset
       is used by tasks downstream of task X. Note that depends_on_past
       is forced to True wherever wait_for_downstream is used. Also note that
       only tasks *immediately* downstream of the previous task instance are waited
       for; the statuses of any tasks further downstream are ignored.
   :type wait_for_downstream: bool
   :param dag: a reference to the dag the task is attached to (if any)
   :type dag: airflow.models.DAG
   :param priority_weight: priority weight of this task against other task.
       This allows the executor to trigger higher priority tasks before
       others when things get backed up. Set priority_weight as a higher
       number for more important tasks.
   :type priority_weight: int
   :param weight_rule: weighting method used for the effective total
       priority weight of the task. Options are:
       ``{ downstream | upstream | absolute }`` default is ``downstream``
       When set to ``downstream`` the effective weight of the task is the
       aggregate sum of all downstream descendants. As a result, upstream
       tasks will have higher weight and will be scheduled more aggressively
       when using positive weight values. This is useful when you have
       multiple dag run instances and desire to have all upstream tasks to
       complete for all runs before each dag can continue processing
       downstream tasks. When set to ``upstream`` the effective weight is the
       aggregate sum of all upstream ancestors. This is the opposite where
       downstream tasks have higher weight and will be scheduled more
       aggressively when using positive weight values. This is useful when you
       have multiple dag run instances and prefer to have each dag complete
       before starting upstream tasks of other dags.  When set to
       ``absolute``, the effective weight is the exact ``priority_weight``
       specified without additional weighting. You may want to do this when
       you know exactly what priority weight each task should have.
       Additionally, when set to ``absolute``, there is bonus effect of
       significantly speeding up the task creation process as for very large
       DAGS. Options can be set as string or using the constants defined in
       the static class ``airflow.utils.WeightRule``
   :type weight_rule: str
   :param queue: which queue to target when running this job. Not
       all executors implement queue management, the CeleryExecutor
       does support targeting specific queues.
   :type queue: str
   :param pool: the slot pool this task should run in, slot pools are a
       way to limit concurrency for certain tasks
   :type pool: str
   :param pool_slots: the number of pool slots this task should use (>= 1)
       Values less than 1 are not allowed.
   :type pool_slots: int
   :param sla: time by which the job is expected to succeed. Note that
       this represents the ``timedelta`` after the period is closed. For
       example if you set an SLA of 1 hour, the scheduler would send an email
       soon after 1:00AM on the ``2016-01-02`` if the ``2016-01-01`` instance
       has not succeeded yet.
       The scheduler pays special attention for jobs with an SLA and
       sends alert
       emails for sla misses. SLA misses are also recorded in the database
       for future reference. All tasks that share the same SLA time
       get bundled in a single email, sent soon after that time. SLA
       notification are sent once and only once for each task instance.
   :type sla: datetime.timedelta
   :param execution_timeout: max time allowed for the execution of
       this task instance, if it goes beyond it will raise and fail.
   :type execution_timeout: datetime.timedelta
   :param on_failure_callback: a function to be called when a task instance
       of this task fails. a context dictionary is passed as a single
       parameter to this function. Context contains references to related
       objects to the task instance and is documented under the macros
       section of the API.
   :type on_failure_callback: TaskStateChangeCallback
   :param on_execute_callback: much like the ``on_failure_callback`` except
       that it is executed right before the task is executed.
   :type on_execute_callback: TaskStateChangeCallback
   :param on_retry_callback: much like the ``on_failure_callback`` except
       that it is executed when retries occur.
   :type on_retry_callback: TaskStateChangeCallback
   :param on_success_callback: much like the ``on_failure_callback`` except
       that it is executed when the task succeeds.
   :type on_success_callback: TaskStateChangeCallback
   :param trigger_rule: defines the rule by which dependencies are applied
       for the task to get triggered. Options are:
       ``{ all_success | all_failed | all_done | one_success |
       one_failed | none_failed | none_failed_or_skipped | none_skipped | dummy}``
       default is ``all_success``. Options can be set as string or
       using the constants defined in the static class
       ``airflow.utils.TriggerRule``
   :type trigger_rule: str
   :param resources: A map of resource parameter names (the argument names of the
       Resources constructor) to their values.
   :type resources: dict
   :param run_as_user: unix username to impersonate while running the task
   :type run_as_user: str
   :param task_concurrency: When set, a task will be able to limit the concurrent
       runs across execution_dates
   :type task_concurrency: int
   :param executor_config: Additional task-level configuration parameters that are
       interpreted by a specific executor. Parameters are namespaced by the name of
       executor.

       **Example**: to run this task in a specific docker container through
       the KubernetesExecutor ::

           MyOperator(...,
               executor_config={
                   "KubernetesExecutor":
                       {"image": "myCustomDockerImage"}
               }
           )

   :type executor_config: dict
   :param do_xcom_push: if True, an XCom is pushed containing the Operator's
       result
   :type do_xcom_push: bool

   .. attribute:: template_fields
      :annotation: :Iterable[str] = []

      

   .. attribute:: template_ext
      :annotation: :Iterable[str] = []

      

   .. attribute:: template_fields_renderers
      :annotation: :Dict[str, str]

      

   .. attribute:: ui_color
      :annotation: :str = #fff

      

   .. attribute:: ui_fgcolor
      :annotation: :str = #000

      

   .. attribute:: pool
      :annotation: :str = 

      

   .. attribute:: _base_operator_shallow_copy_attrs
      :annotation: :Tuple[str, ...] = ['user_defined_macros', 'user_defined_filters', 'params', '_log']

      

   .. attribute:: shallow_copy_attrs
      :annotation: :Tuple[str, ...] = []

      

   .. attribute:: operator_extra_links
      :annotation: :Iterable['BaseOperatorLink'] = []

      

   .. attribute:: __serialized_fields
      :annotation: :Optional[FrozenSet[str]]

      

   .. attribute:: _comps
      

      

   .. attribute:: supports_lineage
      :annotation: = False

      

   .. attribute:: __instantiated
      :annotation: = False

      

   .. attribute:: _lock_for_execution
      :annotation: = False

      

   .. attribute:: dag
      

      Returns the Operator's DAG if set, otherwise raises an error


   .. attribute:: dag_id
      

      Returns dag id if it has one or an adhoc + owner


   .. attribute:: deps
      

      Returns the set of dependencies for the operator. These differ from execution
      context dependencies in that they are specific to tasks and can be
      extended/overridden by subclasses.


   .. attribute:: priority_weight_total
      

      Total priority weight for the task. It might include all upstream or downstream tasks.
      depending on the weight rule.

        - WeightRule.ABSOLUTE - only own weight
        - WeightRule.DOWNSTREAM - adds priority weight of all downstream tasks
        - WeightRule.UPSTREAM - adds priority weight of all upstream tasks


   .. attribute:: upstream_list
      

      @property: list of tasks directly upstream


   .. attribute:: upstream_task_ids
      

      @property: set of ids of tasks directly upstream


   .. attribute:: downstream_list
      

      @property: list of tasks directly downstream


   .. attribute:: downstream_task_ids
      

      @property: set of ids of tasks directly downstream


   .. attribute:: task_type
      

      @property: type of the task


   .. attribute:: roots
      

      Required by TaskMixin


   .. attribute:: leaves
      

      Required by TaskMixin


   .. attribute:: output
      

      Returns reference to XCom pushed by current operator


   
   .. method:: __eq__(self, other)



   
   .. method:: __ne__(self, other)



   
   .. method:: __hash__(self)



   
   .. method:: __or__(self, other)

      Called for [This Operator] | [Operator], The inlets of other
      will be set to pickup the outlets from this operator. Other will
      be set as a downstream task of this operator.



   
   .. method:: __gt__(self, other)

      Called for [Operator] > [Outlet], so that if other is an attr annotated object
      it is set as an outlet of this Operator.



   
   .. method:: __lt__(self, other)

      Called for [Inlet] > [Operator] or [Operator] < [Inlet], so that if other is
      an attr annotated object it is set as an inlet to this operator



   
   .. method:: __setattr__(self, key, value)



   
   .. method:: add_inlets(self, inlets: Iterable[Any])

      Sets inlets to this operator



   
   .. method:: add_outlets(self, outlets: Iterable[Any])

      Defines the outlets of this operator



   
   .. method:: get_inlet_defs(self)

      :return: list of inlets defined for this operator



   
   .. method:: get_outlet_defs(self)

      :return: list of outlets defined for this operator



   
   .. method:: has_dag(self)

      Returns True if the Operator has been assigned to a DAG.



   
   .. method:: prepare_for_execution(self)

      Lock task for execution to disable custom action in __setattr__ and
      returns a copy of the task



   
   .. method:: set_xcomargs_dependencies(self)

      Resolves upstream dependencies of a task. In this way passing an ``XComArg``
      as value for a template field will result in creating upstream relation between
      two tasks.

      **Example**: ::

          with DAG(...):
              generate_content = GenerateContentOperator(task_id="generate_content")
              send_email = EmailOperator(..., html_content=generate_content.output)

          # This is equivalent to
          with DAG(...):
              generate_content = GenerateContentOperator(task_id="generate_content")
              send_email = EmailOperator(
                  ..., html_content="{{ task_instance.xcom_pull('generate_content') }}"
              )
              generate_content >> send_email



   
   .. method:: operator_extra_link_dict(self)

      Returns dictionary of all extra links for the operator



   
   .. method:: global_operator_extra_link_dict(self)

      Returns dictionary of all global extra links



   
   .. method:: pre_execute(self, context: Any)

      This hook is triggered right before self.execute() is called.



   
   .. method:: execute(self, context: Any)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



   
   .. method:: post_execute(self, context: Any, result: Any = None)

      This hook is triggered right after self.execute() is called.
      It is passed the execution context and any results returned by the
      operator.



   
   .. method:: on_kill(self)

      Override this method to cleanup subprocesses when a task instance
      gets killed. Any use of the threading, subprocess or multiprocessing
      module within an operator needs to be cleaned up or it will leave
      ghost processes behind.



   
   .. method:: __deepcopy__(self, memo)

      Hack sorting double chained task lists by task_id to avoid hitting
      max_depth on deepcopy operations.



   
   .. method:: __getstate__(self)



   
   .. method:: __setstate__(self, state)



   
   .. method:: render_template_fields(self, context: Dict, jinja_env: Optional[jinja2.Environment] = None)

      Template all attributes listed in template_fields. Note this operation is irreversible.

      :param context: Dict with values to apply on content
      :type context: dict
      :param jinja_env: Jinja environment
      :type jinja_env: jinja2.Environment



   
   .. method:: _do_render_template_fields(self, parent: Any, template_fields: Iterable[str], context: Dict, jinja_env: jinja2.Environment, seen_oids: Set)



   
   .. method:: render_template(self, content: Any, context: Dict, jinja_env: Optional[jinja2.Environment] = None, seen_oids: Optional[Set] = None)

      Render a templated string. The content can be a collection holding multiple templated strings and will
      be templated recursively.

      :param content: Content to template. Only strings can be templated (may be inside collection).
      :type content: Any
      :param context: Dict with values to apply on templated content
      :type context: dict
      :param jinja_env: Jinja environment. Can be provided to avoid re-creating Jinja environments during
          recursion.
      :type jinja_env: jinja2.Environment
      :param seen_oids: template fields already rendered (to avoid RecursionError on circular dependencies)
      :type seen_oids: set
      :return: Templated content



   
   .. method:: _render_nested_template_fields(self, content: Any, context: Dict, jinja_env: jinja2.Environment, seen_oids: Set)



   
   .. method:: get_template_env(self)

      Fetch a Jinja template environment from the DAG or instantiate empty environment if no DAG.



   
   .. method:: prepare_template(self)

      Hook that is triggered after the templated fields get replaced
      by their content. If you need your operator to alter the
      content of the file before the template is rendered,
      it should override this method to do so.



   
   .. method:: resolve_template_files(self)

      Getting the content of files for template_field / template_ext



   
   .. method:: clear(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, upstream: bool = False, downstream: bool = False, session: Session = None)

      Clears the state of task instances associated with the task, following
      the parameters specified.



   
   .. method:: get_task_instances(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, session: Session = None)

      Get a set of task instance related to this task for a specific date
      range.



   
   .. method:: get_flat_relative_ids(self, upstream: bool = False, found_descendants: Optional[Set[str]] = None)

      Get a flat set of relatives' ids, either upstream or downstream.



   
   .. method:: get_flat_relatives(self, upstream: bool = False)

      Get a flat list of relatives, either upstream or downstream.



   
   .. method:: run(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, ignore_first_depends_on_past: bool = True, ignore_ti_state: bool = False, mark_success: bool = False)

      Run a set of task instances for a date range.



   
   .. method:: dry_run(self)

      Performs dry run for the operator - just render template fields.



   
   .. method:: get_direct_relative_ids(self, upstream: bool = False)

      Get set of the direct relative ids to the current task, upstream or
      downstream.



   
   .. method:: get_direct_relatives(self, upstream: bool = False)

      Get list of the direct relatives to the current task, upstream or
      downstream.



   
   .. method:: __repr__(self)



   
   .. method:: add_only_new(self, item_set: Set[str], item: str)

      Adds only new items to item set



   
   .. method:: _set_relatives(self, task_or_task_list: Union[TaskMixin, Sequence[TaskMixin]], upstream: bool = False)

      Sets relatives for the task or task list.



   
   .. method:: set_downstream(self, task_or_task_list: Union[TaskMixin, Sequence[TaskMixin]])

      Set a task or a task list to be directly downstream from the current
      task. Required by TaskMixin.



   
   .. method:: set_upstream(self, task_or_task_list: Union[TaskMixin, Sequence[TaskMixin]])

      Set a task or a task list to be directly upstream from the current
      task. Required by TaskMixin.



   
   .. staticmethod:: xcom_push(context: Any, key: str, value: Any, execution_date: Optional[datetime] = None)

      Make an XCom available for tasks to pull.

      :param context: Execution Context Dictionary
      :type: Any
      :param key: A key for the XCom
      :type key: str
      :param value: A value for the XCom. The value is pickled and stored
          in the database.
      :type value: any pickleable object
      :param execution_date: if provided, the XCom will not be visible until
          this date. This can be used, for example, to send a message to a
          task on a future date without it being immediately visible.
      :type execution_date: datetime



   
   .. staticmethod:: xcom_pull(context: Any, task_ids: Optional[List[str]] = None, dag_id: Optional[str] = None, key: str = XCOM_RETURN_KEY, include_prior_dates: Optional[bool] = None)

      Pull XComs that optionally meet certain criteria.

      The default value for `key` limits the search to XComs
      that were returned by other tasks (as opposed to those that were pushed
      manually). To remove this filter, pass key=None (or any desired value).

      If a single task_id string is provided, the result is the value of the
      most recent matching XCom from that task_id. If multiple task_ids are
      provided, a tuple of matching values is returned. None is returned
      whenever no matches are found.

      :param context: Execution Context Dictionary
      :type: Any
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



   
   .. method:: extra_links(self)

      @property: extra links for the task



   
   .. method:: get_extra_links(self, dttm: datetime, link_name: str)

      For an operator, gets the URL that the external links specified in
      `extra_links` should point to.

      :raise ValueError: The error message of a ValueError will be passed on through to
          the fronted to show up as a tooltip on the disabled link
      :param dttm: The datetime parsed execution date for the URL being searched for
      :param link_name: The name of the link we're looking for the URL for. Should be
          one of the options specified in `extra_links`
      :return: A URL



   
   .. classmethod:: get_serialized_fields(cls)

      Stringified DAGs and operators contain exactly these fields.



   
   .. method:: is_smart_sensor_compatible(self)

      Return if this operator can use smart service. Default False.




.. py:class:: BaseOperatorLink

   Abstract base class that defines how we get an operator link.

   .. attribute:: operators
      :annotation: :ClassVar[List[Type[BaseOperator]]] = []

      This property will be used by Airflow Plugins to find the Operators to which you want
      to assign this Operator Link

      :return: List of Operator classes used by task for which you want to create extra link


   .. attribute:: name
      

      Name of the link. This will be the button name on the task UI.

      :return: link name


   
   .. method:: get_link(self, operator: BaseOperator, dttm: datetime)

      Link to external system.

      :param operator: airflow operator
      :param dttm: datetime
      :return: link to external system




.. py:class:: Connection(conn_id: Optional[str] = None, conn_type: Optional[str] = None, host: Optional[str] = None, login: Optional[str] = None, password: Optional[str] = None, schema: Optional[str] = None, port: Optional[int] = None, extra: Optional[str] = None, uri: Optional[str] = None)

   Bases: :class:`airflow.models.base.Base`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Placeholder to store information about different database instances
   connection information. The idea here is that scripts use references to
   database instances (conn_id) instead of hard coding hostname, logins and
   passwords when using operators or hooks.

   .. seealso::
       For more information on how to use this class, see: :doc:`/howto/connection/index`

   :param conn_id: The connection ID.
   :type conn_id: str
   :param conn_type: The connection type.
   :type conn_type: str
   :param host: The host.
   :type host: str
   :param login: The login.
   :type login: str
   :param password: The password.
   :type password: str
   :param schema: The schema.
   :type schema: str
   :param port: The port number.
   :type port: int
   :param extra: Extra metadata. Non-standard data such as private/SSH keys can be saved here. JSON
       encoded object.
   :type extra: str
   :param uri: URI address describing connection parameters.
   :type uri: str

   .. attribute:: __tablename__
      :annotation: = connection

      

   .. attribute:: id
      

      

   .. attribute:: conn_id
      

      

   .. attribute:: conn_type
      

      

   .. attribute:: host
      

      

   .. attribute:: schema
      

      

   .. attribute:: login
      

      

   .. attribute:: _password
      

      

   .. attribute:: port
      

      

   .. attribute:: is_encrypted
      

      

   .. attribute:: is_extra_encrypted
      

      

   .. attribute:: _extra
      

      

   .. attribute:: password
      

      Password. The value is decrypted/encrypted when reading/setting the value.


   .. attribute:: extra
      

      Extra data. The value is decrypted/encrypted when reading/setting the value.


   .. attribute:: extra_dejson
      

      Returns the extra property by deserializing json.


   
   .. method:: parse_from_uri(self, **uri)

      This method is deprecated. Please use uri parameter in constructor.



   
   .. method:: _parse_from_uri(self, uri: str)



   
   .. method:: get_uri(self)

      Return connection in URI format



   
   .. method:: get_password(self)

      Return encrypted password.



   
   .. method:: set_password(self, value: Optional[str])

      Encrypt password and set in object attribute.



   
   .. method:: get_extra(self)

      Return encrypted extra-data.



   
   .. method:: set_extra(self, value: str)

      Encrypt extra-data and save in object attribute to object.



   
   .. method:: rotate_fernet_key(self)

      Encrypts data with a new key. See: :ref:`security/fernet`



   
   .. method:: get_hook(self)

      Return hook based on conn_type.



   
   .. method:: __repr__(self)



   
   .. method:: log_info(self)

      This method is deprecated. You can read each field individually or use the
      default representation (`__repr__`).



   
   .. method:: debug_info(self)

      This method is deprecated. You can read each field individually or use the
      default representation (`__repr__`).



   
   .. classmethod:: get_connections_from_secrets(cls, conn_id: str)

      Get all connections as an iterable.

      :param conn_id: connection id
      :return: array of connections




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




.. py:class:: DagTag

   Bases: :class:`airflow.models.base.Base`

   A tag name per dag, to allow quick filtering in the DAG view.

   .. attribute:: __tablename__
      :annotation: = dag_tag

      

   .. attribute:: name
      

      

   .. attribute:: dag_id
      

      

   
   .. method:: __repr__(self)




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




.. py:class:: DagPickle(dag)

   Bases: :class:`airflow.models.base.Base`

   Dags can originate from different places (user repos, master repo, ...)
   and also get executed in different places (different executors). This
   object represents a version of a DAG and becomes a source of truth for
   a BackfillJob execution. A pickle is a native python serialized object,
   and in this case gets stored in the database for the duration of the job.

   The executors pick up the DagPickle id and read the dag definition from
   the database.

   .. attribute:: id
      

      

   .. attribute:: pickle
      

      

   .. attribute:: created_dttm
      

      

   .. attribute:: pickle_hash
      

      

   .. attribute:: __tablename__
      :annotation: = dag_pickle

      


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




.. py:class:: ImportError

   Bases: :class:`airflow.models.base.Base`

   A table to store all Import Errors. The ImportErrors are recorded when parsing DAGs.
   This errors are displayed on the Webserver.

   .. attribute:: __tablename__
      :annotation: = import_error

      

   .. attribute:: id
      

      

   .. attribute:: timestamp
      

      

   .. attribute:: filename
      

      

   .. attribute:: stacktrace
      

      


.. py:class:: Log(event, task_instance=None, owner=None, extra=None, **kwargs)

   Bases: :class:`airflow.models.base.Base`

   Used to actively log events to the database

   .. attribute:: __tablename__
      :annotation: = log

      

   .. attribute:: id
      

      

   .. attribute:: dttm
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: task_id
      

      

   .. attribute:: event
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: owner
      

      

   .. attribute:: extra
      

      

   .. attribute:: __table_args__
      

      


.. py:class:: Pool

   Bases: :class:`airflow.models.base.Base`

   the class to get Pool info.

   .. attribute:: __tablename__
      :annotation: = slot_pool

      

   .. attribute:: id
      

      

   .. attribute:: pool
      

      

   .. attribute:: slots
      

      

   .. attribute:: description
      

      

   .. attribute:: DEFAULT_POOL_NAME
      :annotation: = default_pool

      

   
   .. method:: __repr__(self)



   
   .. staticmethod:: get_pool(pool_name, session: Session = None)

      Get the Pool with specific pool name from the Pools.

      :param pool_name: The pool name of the Pool to get.
      :param session: SQLAlchemy ORM Session
      :return: the pool object



   
   .. staticmethod:: get_default_pool(session: Session = None)

      Get the Pool of the default_pool from the Pools.

      :param session: SQLAlchemy ORM Session
      :return: the pool object



   
   .. staticmethod:: slots_stats(*, lock_rows: bool = False, session: Session = None)

      Get Pool stats (Number of Running, Queued, Open & Total tasks)

      If ``lock_rows`` is True, and the database engine in use supports the ``NOWAIT`` syntax, then a
      non-blocking lock will be attempted -- if the lock is not available then SQLAlchemy will throw an
      OperationalError.

      :param lock_rows: Should we attempt to obtain a row-level lock on all the Pool rows returns
      :param session: SQLAlchemy ORM Session



   
   .. method:: to_json(self)

      Get the Pool in a json structure

      :return: the pool object in json format



   
   .. method:: occupied_slots(self, session: Session)

      Get the number of slots used by running/queued tasks at the moment.

      :param session: SQLAlchemy ORM Session
      :return: the used number of slots



   
   .. method:: running_slots(self, session: Session)

      Get the number of slots used by running tasks at the moment.

      :param session: SQLAlchemy ORM Session
      :return: the used number of slots



   
   .. method:: queued_slots(self, session: Session)

      Get the number of slots used by queued tasks at the moment.

      :param session: SQLAlchemy ORM Session
      :return: the used number of slots



   
   .. method:: open_slots(self, session: Session)

      Get the number of slots open at the moment.

      :param session: SQLAlchemy ORM Session
      :return: the number of slots




.. py:class:: RenderedTaskInstanceFields(ti: TaskInstance, render_templates=True)

   Bases: :class:`airflow.models.base.Base`

   Save Rendered Template Fields

   .. attribute:: __tablename__
      :annotation: = rendered_task_instance_fields

      

   .. attribute:: dag_id
      

      

   .. attribute:: task_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: rendered_fields
      

      

   .. attribute:: k8s_pod_yaml
      

      

   
   .. method:: __repr__(self)



   
   .. classmethod:: get_templated_fields(cls, ti: TaskInstance, session: Session = None)

      Get templated field for a TaskInstance from the RenderedTaskInstanceFields
      table.

      :param ti: Task Instance
      :param session: SqlAlchemy Session
      :return: Rendered Templated TI field



   
   .. classmethod:: get_k8s_pod_yaml(cls, ti: TaskInstance, session: Session = None)

      Get rendered Kubernetes Pod Yaml for a TaskInstance from the RenderedTaskInstanceFields
      table.

      :param ti: Task Instance
      :param session: SqlAlchemy Session
      :return: Kubernetes Pod Yaml



   
   .. method:: write(self, session: Session = None)

      Write instance to database

      :param session: SqlAlchemy Session



   
   .. classmethod:: delete_old_records(cls, task_id: str, dag_id: str, num_to_keep=conf.getint('core', 'max_num_rendered_ti_fields_per_task', fallback=0), session: Session = None)

      Keep only Last X (num_to_keep) number of records for a task by deleting others

      :param task_id: Task ID
      :param dag_id: Dag ID
      :param num_to_keep: Number of Records to keep
      :param session: SqlAlchemy Session




.. py:class:: SensorInstance(ti)

   Bases: :class:`airflow.models.base.Base`

   SensorInstance support the smart sensor service. It stores the sensor task states
   and context that required for poking include poke context and execution context.
   In sensor_instance table we also save the sensor operator classpath so that inside
   smart sensor there is no need to import the dagbag and create task object for each
   sensor task.

   SensorInstance include another set of columns to support the smart sensor shard on
   large number of sensor instance. The key idea is to generate the hash code from the
   poke context and use it to map to a shorter shard code which can be used as an index.
   Every smart sensor process takes care of tasks whose `shardcode` are in a certain range.

   .. attribute:: __tablename__
      :annotation: = sensor_instance

      

   .. attribute:: id
      

      

   .. attribute:: task_id
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: state
      

      

   .. attribute:: _try_number
      

      

   .. attribute:: start_date
      

      

   .. attribute:: operator
      

      

   .. attribute:: op_classpath
      

      

   .. attribute:: hashcode
      

      

   .. attribute:: shardcode
      

      

   .. attribute:: poke_context
      

      

   .. attribute:: execution_context
      

      

   .. attribute:: created_at
      

      

   .. attribute:: updated_at
      

      

   .. attribute:: __table_args__
      

      

   .. attribute:: try_number
      

      Return the try number that this task number will be when it is actually
      run.
      If the TI is currently running, this will match the column in the
      database, in all other cases this will be incremented.


   
   .. staticmethod:: get_classpath(obj)

      Get the object dotted class path. Used for getting operator classpath.

      :param obj:
      :type obj:
      :return: The class path of input object
      :rtype: str



   
   .. classmethod:: register(cls, ti, poke_context, execution_context, session=None)

      Register task instance ti for a sensor in sensor_instance table. Persist the
      context used for a sensor and set the sensor_instance table state to sensing.

      :param ti: The task instance for the sensor to be registered.
      :type: ti:
      :param poke_context: Context used for sensor poke function.
      :type poke_context: dict
      :param execution_context: Context used for execute sensor such as timeout
          setting and email configuration.
      :type execution_context: dict
      :param session: SQLAlchemy ORM Session
      :type session: Session
      :return: True if the ti was registered successfully.
      :rtype: Boolean



   
   .. method:: __repr__(self)




.. py:class:: SkipMixin

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   A Mixin to skip Tasks Instances

   
   .. method:: _set_state_to_skipped(self, dag_run, execution_date, tasks, session)

      Used internally to set state of task instances to skipped from the same dag run.



   
   .. method:: skip(self, dag_run, execution_date, tasks, session=None)

      Sets tasks instances to skipped from the same dag run.

      If this instance has a `task_id` attribute, store the list of skipped task IDs to XCom
      so that NotPreviouslySkippedDep knows these tasks should be skipped when they
      are cleared.

      :param dag_run: the DagRun for which to set the tasks to skipped
      :param execution_date: execution_date
      :param tasks: tasks to skip (not task_ids)
      :param session: db session to use



   
   .. method:: skip_all_except(self, ti: TaskInstance, branch_task_ids: Union[str, Iterable[str]])

      This method implements the logic for a branching operator; given a single
      task ID or list of task IDs to follow, this skips all other tasks
      immediately downstream of this operator.

      branch_task_ids is stored to XCom so that NotPreviouslySkippedDep knows skipped tasks or
      newly added tasks should be skipped when they are cleared.




.. py:class:: SlaMiss

   Bases: :class:`airflow.models.base.Base`

   Model that stores a history of the SLA that have been missed.
   It is used to keep track of SLA failures over time and to avoid double
   triggering alert emails.

   .. attribute:: __tablename__
      :annotation: = sla_miss

      

   .. attribute:: task_id
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: email_sent
      

      

   .. attribute:: timestamp
      

      

   .. attribute:: description
      

      

   .. attribute:: notification_sent
      

      

   .. attribute:: __table_args__
      

      

   
   .. method:: __repr__(self)




.. py:class:: TaskFail(task, execution_date, start_date, end_date)

   Bases: :class:`airflow.models.base.Base`

   TaskFail tracks the failed run durations of each task instance.

   .. attribute:: __tablename__
      :annotation: = task_fail

      

   .. attribute:: id
      

      

   .. attribute:: task_id
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: start_date
      

      

   .. attribute:: end_date
      

      

   .. attribute:: duration
      

      

   .. attribute:: __table_args__
      

      


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




.. function:: clear_task_instances(tis, session, activate_dag_runs=True, dag=None)
   Clears a set of task instances, but makes sure the running ones
   get killed.

   :param tis: a list of task instances
   :param session: current session
   :param activate_dag_runs: flag to check for active dag run
   :param dag: DAG object


.. py:class:: TaskReschedule(task, execution_date, try_number, start_date, end_date, reschedule_date)

   Bases: :class:`airflow.models.base.Base`

   TaskReschedule tracks rescheduled task instances.

   .. attribute:: __tablename__
      :annotation: = task_reschedule

      

   .. attribute:: id
      

      

   .. attribute:: task_id
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: try_number
      

      

   .. attribute:: start_date
      

      

   .. attribute:: end_date
      

      

   .. attribute:: duration
      

      

   .. attribute:: reschedule_date
      

      

   .. attribute:: __table_args__
      

      

   
   .. staticmethod:: query_for_task_instance(task_instance, descending=False, session=None)

      Returns query for task reschedules for a given the task instance.

      :param session: the database session object
      :type session: sqlalchemy.orm.session.Session
      :param task_instance: the task instance to find task reschedules for
      :type task_instance: airflow.models.TaskInstance
      :param descending: If True then records are returned in descending order
      :type descending: bool



   
   .. staticmethod:: find_for_task_instance(task_instance, session=None)

      Returns all task reschedules for the task instance and try number,
      in ascending order.

      :param session: the database session object
      :type session: sqlalchemy.orm.session.Session
      :param task_instance: the task instance to find task reschedules for
      :type task_instance: airflow.models.TaskInstance




.. py:class:: Variable(key=None, val=None)

   Bases: :class:`airflow.models.base.Base`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Variables are a generic way to store and retrieve arbitrary content or settings
   as a simple key value store within Airflow.

   .. attribute:: __tablename__
      :annotation: = variable

      

   .. attribute:: __NO_DEFAULT_SENTINEL
      

      

   .. attribute:: id
      

      

   .. attribute:: key
      

      

   .. attribute:: _val
      

      

   .. attribute:: is_encrypted
      

      

   .. attribute:: val
      

      Get Airflow Variable from Metadata DB and decode it using the Fernet Key


   
   .. method:: __repr__(self)



   
   .. method:: get_val(self)

      Get Airflow Variable from Metadata DB and decode it using the Fernet Key



   
   .. method:: set_val(self, value)

      Encode the specified value with Fernet Key and store it in Variables Table.



   
   .. classmethod:: setdefault(cls, key, default, deserialize_json=False)

      Like a Python builtin dict object, setdefault returns the current value
      for a key, and if it isn't there, stores the default value and returns it.

      :param key: Dict key for this Variable
      :type key: str
      :param default: Default value to set and return if the variable
          isn't already in the DB
      :type default: Mixed
      :param deserialize_json: Store this as a JSON encoded value in the DB
          and un-encode it when retrieving a value
      :return: Mixed



   
   .. classmethod:: get(cls, key: str, default_var: Any = __NO_DEFAULT_SENTINEL, deserialize_json: bool = False)

      Sets a value for an Airflow Key

      :param key: Variable Key
      :param default_var: Default value of the Variable if the Variable doesn't exists
      :param deserialize_json: Deserialize the value to a Python dict



   
   .. classmethod:: set(cls, key: str, value: Any, serialize_json: bool = False, session: Session = None)

      Sets a value for an Airflow Variable with a given Key

      :param key: Variable Key
      :param value: Value to set for the Variable
      :param serialize_json: Serialize the value to a JSON string
      :param session: SQL Alchemy Sessions



   
   .. classmethod:: delete(cls, key: str, session: Session = None)

      Delete an Airflow Variable for a given key

      :param key: Variable Key
      :param session: SQL Alchemy Sessions



   
   .. method:: rotate_fernet_key(self)

      Rotate Fernet Key



   
   .. staticmethod:: get_variable_from_secrets(key: str)

      Get Airflow Variable by iterating over all Secret Backends.

      :param key: Variable Key
      :return: Variable Value




.. data:: XCOM_RETURN_KEY
   :annotation: = return_value

   

.. data:: XCom
   

   

