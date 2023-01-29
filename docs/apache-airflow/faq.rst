 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _faq:

FAQ
========

Scheduling / DAG file parsing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Why is task not getting scheduled?
----------------------------------

There are very many reasons why your task might not be getting scheduled. Here are some of the common causes:

- Does your script "compile", can the Airflow engine parse it and find your
  DAG object? To test this, you can run ``airflow dags list`` and
  confirm that your DAG shows up in the list. You can also run
  ``airflow tasks list foo_dag_id --tree`` and confirm that your task
  shows up in the list as expected. If you use the CeleryExecutor, you
  may want to confirm that this works both where the scheduler runs as well
  as where the worker runs.

- Does the file containing your DAG contain the string "airflow" and "DAG" somewhere
  in the contents? When searching the DAG directory, Airflow ignores files not containing
  "airflow" and "DAG" in order to prevent the DagBag parsing from importing all python
  files collocated with user's DAGs.

- Is your ``start_date`` set properly? For time-based DAGs, the task won't be triggered until the
  the first schedule interval following the start date has passed.

- Is your ``schedule`` argument set properly? The default
  is one day (``datetime.timedelta(1)``). You must specify a different ``schedule``
  directly to the DAG object you instantiate, not as a ``default_param``, as task instances
  do not override their parent DAG's ``schedule``.

- Is your ``start_date`` beyond where you can see it in the UI? If you
  set your ``start_date`` to some time say 3 months ago, you won't be able to see
  it in the main view in the UI, but you should be able to see it in the
  ``Menu -> Browse ->Task Instances``.

- Are the dependencies for the task met? The task instances directly
  upstream from the task need to be in a ``success`` state. Also,
  if you have set ``depends_on_past=True``, the previous task instance
  needs to have succeeded (except if it is the first run for that task).
  Also, if ``wait_for_downstream=True``, make sure you understand
  what it means - all tasks *immediately* downstream of the *previous*
  task instance must have succeeded.
  You can view how these properties are set from the ``Task Instance Details``
  page for your task.

- Are the DagRuns you need created and active? A DagRun represents a specific
  execution of an entire DAG and has a state (running, success, failed, ...).
  The scheduler creates new DagRun as it moves forward, but never goes back
  in time to create new ones. The scheduler only evaluates ``running`` DagRuns
  to see what task instances it can trigger. Note that clearing tasks
  instances (from the UI or CLI) does set the state of a DagRun back to
  running. You can bulk view the list of DagRuns and alter states by clicking
  on the schedule tag for a DAG.

- Is the ``concurrency`` parameter of your DAG reached? ``concurrency`` defines
  how many ``running`` task instances a DAG is allowed to have, beyond which
  point things get queued.

- Is the ``max_active_runs`` parameter of your DAG reached? ``max_active_runs`` defines
  how many ``running`` concurrent instances of a DAG there are allowed to be.

You may also want to read about the :ref:`scheduler` and make
sure you fully understand how the scheduler cycle.


How to improve DAG performance?
-------------------------------

There are some Airflow configuration to allow for a larger scheduling capacity and frequency:

- :ref:`config:core__parallelism`
- :ref:`config:core__max_active_tasks_per_dag`
- :ref:`config:core__max_active_runs_per_dag`

DAGs have configurations that improves efficiency:

- ``max_active_tasks``: Overrides :ref:`config:core__max_active_tasks_per_dag`.
- ``max_active_runs``: Overrides :ref:`config:core__max_active_runs_per_dag`.

Operators or tasks also have configurations that improves efficiency and scheduling priority:

- ``max_active_tis_per_dag``: This parameter controls the number of concurrent running task instances across ``dag_runs``
  per task.
- ``pool``: See :ref:`concepts:pool`.
- ``priority_weight``: See :ref:`concepts:priority-weight`.
- ``queue``: See :ref:`executor:CeleryExecutor:queue` for CeleryExecutor deployments only.


How to reduce DAG scheduling latency / task delay?
--------------------------------------------------

Airflow 2.0 has low DAG scheduling latency out of the box (particularly when compared with Airflow 1.10.x),
however if you need more throughput you can :ref:`start multiple schedulers<scheduler:ha>`.


How do I trigger tasks based on another task's failure?
-------------------------------------------------------

You can achieve this with :ref:`concepts:trigger-rules`.

How to control DAG file parsing timeout for different DAG files?
----------------------------------------------------------------

(only valid for Airflow >= 2.3.0)

You can add a ``get_dagbag_import_timeout`` function in your ``airflow_local_settings.py`` which gets
called right before a DAG file is parsed. You can return different timeout value based on the DAG file.
When the return value is less than or equal to 0, it means no timeout during the DAG parsing.

.. code-block:: python
   :caption: airflow_local_settings.py
   :name: airflow_local_settings.py

    def get_dagbag_import_timeout(dag_file_path: str) -> Union[int, float]:
        """
        This setting allows to dynamically control the DAG file parsing timeout.

        It is useful when there are a few DAG files requiring longer parsing times, while others do not.
        You can control them separately instead of having one value for all DAG files.

        If the return value is less than or equal to 0, it means no timeout during the DAG parsing.
        """
        if "slow" in dag_file_path:
            return 90
        if "no-timeout" in dag_file_path:
            return 0
        return conf.getfloat("core", "DAGBAG_IMPORT_TIMEOUT")


When there are a lot (>1000) of DAG files, how to speed up parsing of new files?
---------------------------------------------------------------------------------

(only valid for Airflow >= 2.1.1)

Change the :ref:`config:scheduler__file_parsing_sort_mode` to ``modified_time``, raise
the :ref:`config:scheduler__min_file_process_interval` to ``600`` (10 minutes), ``6000`` (100 minutes)
or a higher value.

The DAG parser will skip the ``min_file_process_interval`` check if a file is recently modified.

This might not work for case where the DAG is imported/created from a separate file. Example:
``dag_file.py`` that imports ``dag_loader.py`` where the actual logic of the DAG file is as shown below.
In this case if ``dag_loader.py`` is updated but ``dag_file.py`` is not updated, the changes won't be reflected
until ``min_file_process_interval`` is reached since DAG Parser will look for modified time for ``dag_file.py`` file.

.. code-block:: python
   :caption: dag_file.py
   :name: dag_file.py

    from dag_loader import create_dag

    globals()[dag.dag_id] = create_dag(dag_id, schedule, dag_number, default_args)

.. code-block:: python
   :caption: dag_loader.py
   :name: dag_loader.py

    from airflow import DAG
    from airflow.decorators import task

    import pendulum


    def create_dag(dag_id, schedule, dag_number, default_args):
        dag = DAG(
            dag_id,
            schedule=schedule,
            default_args=default_args,
            pendulum.datetime(2021, 9, 13, tz="UTC"),
        )

        with dag:

            @task()
            def hello_world():
                print("Hello World")
                print(f"This is DAG: {dag_number}")

            hello_world()

        return dag


DAG construction
^^^^^^^^^^^^^^^^

What's the deal with ``start_date``?
------------------------------------

``start_date`` is partly legacy from the pre-DagRun era, but it is still
relevant in many ways. When creating a new DAG, you probably want to set
a global ``start_date`` for your tasks. This can be done by declaring your
``start_date`` directly in the ``DAG()`` object. The first
DagRun to be created will be based on the ``min(start_date)`` for all your
tasks. From that point on, the scheduler creates new DagRuns based on
your ``schedule`` and the corresponding task instances run as your
dependencies are met. When introducing new tasks to your DAG, you need to
pay special attention to ``start_date``, and may want to reactivate
inactive DagRuns to get the new task onboarded properly.

We recommend against using dynamic values as ``start_date``, especially
``datetime.now()`` as it can be quite confusing. The task is triggered
once the period closes, and in theory an ``@hourly`` DAG would never get to
an hour after now as ``now()`` moves along.


Previously, we also recommended using rounded ``start_date`` in relation to your
DAG's ``schedule``. This meant an ``@hourly`` would be at ``00:00``
minutes:seconds, a ``@daily`` job at midnight, a ``@monthly`` job on the
first of the month. This is no longer required. Airflow will now auto align
the ``start_date`` and the ``schedule``, by using the ``start_date``
as the moment to start looking.

You can use any sensor or a ``TimeDeltaSensor`` to delay
the execution of tasks within the schedule interval.
While ``schedule`` does allow specifying a ``datetime.timedelta``
object, we recommend using the macros or cron expressions instead, as
it enforces this idea of rounded schedules.

When using ``depends_on_past=True``, it's important to pay special attention
to ``start_date``, as the past dependency is not enforced only on the specific
schedule of the ``start_date`` specified for the task. It's also
important to watch DagRun activity status in time when introducing
new ``depends_on_past=True``, unless you are planning on running a backfill
for the new task(s).

It is also important to note that the task's ``start_date``, in the context of a
backfill CLI command, gets overridden by the backfill's ``start_date`` commands.
This allows for a backfill on tasks that have ``depends_on_past=True`` to
actually start. If this were not the case, the backfill just would not start.

Using time zones
----------------

Creating a time zone aware datetime (e.g. DAG's ``start_date``) is quite simple. Just make sure to supply
a time zone aware dates using ``pendulum``. Don't try to use standard library
`timezone <https://docs.python.org/3/library/datetime.html#timezone-objects>`_ as they are known to
have limitations and we deliberately disallow using them in DAGs.


.. _faq:what-does-execution-date-mean:

What does ``execution_date`` mean?
----------------------------------

*Execution date* or ``execution_date`` is a historical name for what is called a
*logical date*, and also usually the start of the data interval represented by a
DAG run.

Airflow was developed as a solution for ETL needs. In the ETL world, you
typically summarize data. So, if you want to summarize data for ``2016-02-19``,
you would do it at ``2016-02-20`` midnight UTC, which would be right after all
data for ``2016-02-19`` becomes available. This interval between midnights of
``2016-02-19`` and ``2016-02-20`` is called the *data interval*, and since it
represents data in the date of ``2016-02-19``, this date is also called the
run's *logical date*, or the date that this DAG run is executed for, thus
*execution date*.

For backward compatibility, a datetime value ``execution_date`` is still
as :ref:`Template variables<templates:variables>` with various formats in Jinja
templated fields, and in Airflow's Python API. It is also included in the
context dictionary given to an Operator's execute function.

.. code-block:: python

        class MyOperator(BaseOperator):
            def execute(self, context):
                logging.info(context["execution_date"])

However, you should always use ``data_interval_start`` or ``data_interval_end``
if possible, since those names are semantically more correct and less prone to
misunderstandings.

Note that ``ds`` (the YYYY-MM-DD form of ``data_interval_start``) refers to
*date* ***string***, not *date* ***start*** as may be confusing to some.

.. tip::

    For more information on ``logical date``, see :ref:`data-interval` and
    :ref:`concepts-dag-run`.


How to create DAGs dynamically?
-------------------------------

Airflow looks in your ``DAGS_FOLDER`` for modules that contain ``DAG`` objects
in their global namespace and adds the objects it finds in the
``DagBag``. Knowing this, all we need is a way to dynamically assign
variable in the global namespace. This is easily done in python using the
``globals()`` function for the standard library, which behaves like a
simple dictionary.

.. code-block:: python

    def create_dag(dag_id):
        """
        A function returning a DAG object.
        """

        return DAG(dag_id)


    for i in range(10):
        dag_id = f"foo_{i}"
        globals()[dag_id] = DAG(dag_id)

        # or better, call a function that returns a DAG object!
        other_dag_id = f"bar_{i}"
        globals()[other_dag_id] = create_dag(other_dag_id)

Even though Airflow supports multiple DAG definition per python file, dynamically generated or otherwise, it is not
recommended as Airflow would like better isolation between DAGs from a fault and deployment perspective and multiple
DAGs in the same file goes against that.


Are top level Python code allowed?
----------------------------------

While it is not recommended to write any code outside of defining Airflow constructs, Airflow does support any
arbitrary python code as long as it does not break the DAG file processor or prolong file processing time past the
:ref:`config:core__dagbag_import_timeout` value.

A common example is the violation of the time limit when building a dynamic DAG which usually requires querying data
from another service like a database. At the same time, the requested service is being swamped with DAG file
processors requests for data to process the file. These unintended interactions may cause the service to deteriorate
and eventually cause DAG file processing to fail.

Refer to :ref:`DAG writing best practices<best_practice:writing_a_dag>` for more information.


Do Macros resolves in another Jinja template?
---------------------------------------------

It is not possible to render :ref:`Macros<macros>` or any Jinja template within another Jinja template. This is
commonly attempted in ``user_defined_macros``.

.. code-block:: python

        dag = DAG(
            # ...
            user_defined_macros={"my_custom_macro": "day={{ ds }}"}
        )

        bo = BashOperator(task_id="my_task", bash_command="echo {{ my_custom_macro }}", dag=dag)

This will echo "day={{ ds }}" instead of "day=2020-01-01" for a DAG run with a
``data_interval_start`` of 2020-01-01 00:00:00.

.. code-block:: python

        bo = BashOperator(task_id="my_task", bash_command="echo day={{ ds }}", dag=dag)

By using the ds macros directly in the template_field, the rendered value results in "day=2020-01-01".


Why ``next_ds`` or ``prev_ds`` might not contain expected values?
------------------------------------------------------------------

- When scheduling DAG, the ``next_ds`` ``next_ds_nodash`` ``prev_ds`` ``prev_ds_nodash`` are calculated using
  ``logical_date`` and the DAG's schedule (if applicable). If you set ``schedule`` as ``None`` or ``@once``,
  the ``next_ds``, ``next_ds_nodash``, ``prev_ds``, ``prev_ds_nodash`` values will be set to ``None``.
- When manually triggering DAG, the schedule will be ignored, and ``prev_ds == next_ds == ds``.


Task execution interactions
^^^^^^^^^^^^^^^^^^^^^^^^^^^

What does ``TemplateNotFound`` mean?
-------------------------------------

``TemplateNotFound`` errors are usually due to misalignment with user expectations when passing path to operator
that trigger Jinja templating. A common occurrence is with :ref:`BashOperators<howto/operator:BashOperator>`.

Another commonly missed fact is that the files are resolved relative to where the pipeline file lives. You can add
other directories to the ``template_searchpath`` of the DAG object to allow for other non-relative location.


How to trigger tasks based on another task's failure?
-----------------------------------------------------

For tasks that are related through dependency, you can set the ``trigger_rule`` to ``TriggerRule.ALL_FAILED`` if the
task execution depends on the failure of ALL its upstream tasks or ``TriggerRule.ONE_FAILED`` for just one of the
upstream task.

.. code-block:: python

    import pendulum

    from airflow.decorators import dag, task
    from airflow.exceptions import AirflowException
    from airflow.utils.trigger_rule import TriggerRule


    @task()
    def a_func():
        raise AirflowException


    @task(
        trigger_rule=TriggerRule.ALL_FAILED,
    )
    def b_func():
        pass


    @dag(schedule="@once", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"))
    def my_dag():
        a = a_func()
        b = b_func()

        a >> b


    dag = my_dag()

See :ref:`concepts:trigger-rules` for more information.

If the tasks are not related by dependency, you will need to :ref:`build a custom Operator<custom_operator>`.

Airflow UI
^^^^^^^^^^

How do I stop the sync perms happening multiple times per webserver?
--------------------------------------------------------------------

Set the value of ``update_fab_perms`` configuration in ``airflow.cfg`` to ``False``.


How to reduce the airflow UI page load time?
------------------------------------------------

If your DAG takes long time to load, you could reduce the value of ``default_dag_run_display_number`` configuration
in ``airflow.cfg`` to a smaller value. This configurable controls the number of DAG runs to show in UI with default
value ``25``.


Why did the pause DAG toggle turn red?
--------------------------------------

If pausing or unpausing a DAG fails for any reason, the DAG toggle will
revert to its previous state and turn red. If you observe this behavior,
try pausing the DAG again, or check the console or server logs if the
issue recurs.


MySQL and MySQL variant Databases
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

What does "MySQL Server has gone away" mean?
--------------------------------------------

You may occasionally experience ``OperationalError`` with the message "MySQL Server has gone away". This is due to the
connection pool keeping connections open too long and you are given an old connection that has expired. To ensure a
valid connection, you can set :ref:`config:core__sql_alchemy_pool_recycle` to ensure connections are invalidated after
that many seconds and new ones are created.


Does Airflow support extended ASCII or unicode characters?
----------------------------------------------------------

If you intend to use extended ASCII or Unicode characters in Airflow, you have to provide a proper connection string to
the MySQL database since they define charset explicitly.

.. code-block:: text

    sql_alchemy_conn = mysql://airflow@localhost:3306/airflow?charset=utf8

You will experience ``UnicodeDecodeError`` thrown by ``WTForms`` templating and other Airflow modules like below.

.. code-block:: text

   'ascii' codec can't decode byte 0xae in position 506: ordinal not in range(128)


How to fix Exception: Global variable ``explicit_defaults_for_timestamp`` needs to be on (1)?
---------------------------------------------------------------------------------------------

This means ``explicit_defaults_for_timestamp`` is disabled in your mysql server and you need to enable it by:

#. Set ``explicit_defaults_for_timestamp = 1`` under the ``mysqld`` section in your ``my.cnf`` file.
#. Restart the Mysql server.
