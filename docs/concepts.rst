Concepts
########

The Airflow Platform is a tool for describing, executing, and monitoring
workflows.

Core Ideas
''''''''''

DAGs
====

In Airflow, a ``DAG`` -- or a Directed Acyclic Graph -- is a collection of all
the tasks you want to run, organized in a way that reflects their relationships
and dependencies.

For example, a simple DAG could consist of three tasks: A, B, and C. It could
say that A has to run successfully before B can run, but C can run anytime. It
could say that task A times out after 5 minutes, and B can be restarted up to 5
times in case it fails. It might also say that the workflow will run every night
at 10pm, but shouldn't start until a certain date.

In this way, a DAG describes *how* you want to carry out your workflow; but
notice that we haven't said anything about *what* we actually want to do! A, B,
and C could be anything. Maybe A prepares data for B to analyze while C sends an
email. Or perhaps A monitors your location so B can open your garage door while
C turns on your house lights. The important thing is that the DAG isn't
concerned with what its constituent tasks do; its job is to make sure that
whatever they do happens at the right time, or in the right order, or with the
right handling of any unexpected issues.

DAGs are defined in standard Python files that are placed in Airflow's
``DAG_FOLDER``. Airflow will execute the code in each file to dynamically build
the ``DAG`` objects. You can have as many DAGs as you want, each describing an
arbitrary number of tasks. In general, each one should correspond to a single
logical workflow.

Scope
-----

Airflow will load any ``DAG`` object it can import from a DAGfile. Critically,
that means the DAG must appear in ``globals()``. Consider the following two
DAGs. Only ``dag_1`` will be loaded; the other one only appears in a local
scope.

.. code:: python

    dag_1 = DAG('this_dag_will_be_discovered')

    def my_function()
        dag_2 = DAG('but_this_dag_will_not')

    my_function()

Sometimes this can be put to good use. For example, a common pattern with
``SubDagOperator`` is to define the subdag inside a function so that Airflow
doesn't try to load it as a standalone DAG.

Default Arguments
-----------------

If a dictionary of ``default_args`` is passed to a DAG, it will apply them to
any of its operators. This makes it easy to apply a common parameter to many operators without having to type it many times.

.. code:: python

    default_args=dict(
        start_date=datetime(2016, 1, 1),
        owner='Airflow')

    dag = DAG('my_dag', default_args=default_args)
    op = DummyOperator(task_id='dummy', dag=dag)
    print(op.owner) # Airflow

Context Manager
---------------

*Added in Airflow 1.8*

DAGs can be used as context managers to automatically assign new operators to that DAG.

.. code:: python

    with DAG('my_dag', start_date=datetime(2016, 1, 1)) as dag:
        op = DummyOperator('op')

    op.dag is dag # True

Operators
=========

While DAGs describe *how* to run a workflow, ``Operators`` determine what
actually gets done.

An operator describes a single task in a workflow. Operators are usually (but
not always) atomic, meaning they can stand on their own and don't need to share
resources with any other operators. The DAG will make sure that operators run in
the correct certain order; other than those dependencies, operators generally
run independently. In fact, they may run on two completely different machines.

This is a subtle but very important point: in general, if two operators need to
share information, like a filename or small amount of data, you should consider
combining them into a single operator. If it absolutely can't be avoided,
Airflow does have a feature for operator cross-communication called XCom that is
described elsewhere in this document.

Airflow provides operators for many common tasks, including:

- ``BashOperator`` - executes a bash command
- ``PythonOperator`` - calls an arbitrary Python function
- ``EmailOperator`` - sends an email
- ``HTTPOperator`` - sends an HTTP request
- ``MySqlOperator``, ``SqliteOperator``, ``PostgresOperator``, ``MsSqlOperator``, ``OracleOperator``, ``JdbcOperator``, etc. - executes a SQL command
- ``Sensor`` - waits for a certain time, file, database row, S3 key, etc...


In addition to these basic building blocks, there are many more specific
operators: ``DockerOperator``, ``HiveOperator``, ``S3FileTransferOperator``,
``PrestoToMysqlOperator``, ``SlackOperator``... you get the idea!

The ``airflow/contrib/`` directory contains yet more operators built by the
community. These operators aren't always as complete or well-tested as those in
the main distribution, but allow users to more easily add new functionality to
the platform.

Operators are only loaded by Airflow if they are assigned to a DAG.

DAG Assignment
--------------

*Added in Airflow 1.8*

Operators do not have to be assigned to DAGs immediately (previously ``dag`` was
a required argument). However, once an operator is assigned to a DAG, it can not
be transferred or unassigned. DAG assignment can be done explicitly when the
operator is created, through deferred assignment, or even inferred from other
operators.

.. code:: python

    dag = DAG('my_dag', start_date=datetime(2016, 1, 1))

    # sets the DAG explicitly
    explicit_op = DummyOperator(task_id='op1', dag=dag)

    # deferred DAG assignment
    deferred_op = DummyOperator(task_id='op2')
    deferred_op.dag = dag

    # inferred DAG assignment (linked operators must be in the same DAG)
    inferred_op = DummyOperator(task_id='op3')
    inferred_op.set_upstream(deferred_op)


Bitshift Composition
--------------------

*Added in Airflow 1.8*

Traditionally, operator relationships are set with the ``set_upstream()`` and
``set_downstream()`` methods. In Airflow 1.8, this can be done with the Python
bitshift operators ``>>`` and ``<<``. The following four statements are all
functionally equivalent:

.. code:: python

    op1 >> op2
    op1.set_downstream(op2)

    op2 << op1
    op2.set_upstream(op1)

When using the bitshift to compose operators, the relationship is set in the
direction that the bitshift operator points. For example, ``op1 >> op2`` means
that ``op1`` runs first and ``op2`` runs second. Multiple operators can be
composed -- keep in mind the chain is executed left-to-right and the rightmost
object is always returned. For example:

.. code:: python

    op1 >> op2 >> op3 << op4

is equivalent to:

.. code:: python

    op1.set_downstream(op2)
    op2.set_downstream(op3)
    op3.set_upstream(op4)

For convenience, the bitshift operators can also be used with DAGs. For example:

.. code:: python

    dag >> op1 >> op2

is equivalent to:

.. code:: python

    op1.dag = dag
    op1.set_downstream(op2)

We can put this all together to build a simple pipeline:

.. code:: python

    with DAG('my_dag', start_date=datetime(2016, 1, 1)) as dag:
        (
            DummyOperator(task_id='dummy_1')
            >> BashOperator(
                task_id='bash_1',
                bash_command='echo "HELLO!"')
            >> PythonOperator(
                task_id='python_1',
                python_callable=lambda: print("GOODBYE!"))
        )

Tasks
=====

Once an operator is instantiated, it is referred to as a "task". The
instantiation defines specific values when calling the abstract operator, and
the parameterized task becomes a node in a DAG.

Task Instances
==============

A task instance represents a specific run of a task and is characterized as the
combination of a dag, a task, and a point in time. Task instances also have an
indicative state, which could be "running", "success", "failed", "skipped", "up
for retry", etc.

Workflows
=========

You're now familiar with the core building blocks of Airflow.
Some of the concepts may sound very similar, but the vocabulary can
be conceptualized like this:

- DAG: a description of the order in which work should take place
- Operator: a class that acts as a template for carrying out some work
- Task: a parameterized instance of an operator
- Task Instance: a task that 1) has been assigned to a DAG and 2) has a
  state associated with a specific run of the DAG

By combining ``DAGs`` and ``Operators`` to create ``TaskInstances``, you can
build complex workflows.

Additional Functionality
''''''''''''''''''''''''

In addition to the core Airflow objects, there are a number of more complex
features that enable behaviors like limiting simultaneous access to resources,
cross-communication, conditional execution, and more.

Hooks
=====

Hooks are interfaces to external platforms and databases like Hive, S3,
MySQL, Postgres, HDFS, and Pig. Hooks implement a common interface when
possible, and act as a building block for operators. They also use
the ``airflow.models.Connection`` model to retrieve hostnames
and authentication information. Hooks keep authentication code and
information out of pipelines, centralized in the metadata database.

Hooks are also very useful on their own to use in Python scripts,
Airflow airflow.operators.PythonOperator, and in interactive environments
like iPython or Jupyter Notebook.

Pools
=====

Some systems can get overwhelmed when too many processes hit them at the same
time. Airflow pools can be used to **limit the execution parallelism** on
arbitrary sets of tasks. The list of pools is managed in the UI
(``Menu -> Admin -> Pools``) by giving the pools a name and assigning
it a number of worker slots. Tasks can then be associated with
one of the existing pools by using the ``pool`` parameter when
creating tasks (i.e., instantiating operators).

.. code:: python

    aggregate_db_message_job = BashOperator(
        task_id='aggregate_db_message_job',
        execution_timeout=timedelta(hours=3),
        pool='ep_data_pipeline_db_msg_agg',
        bash_command=aggregate_db_message_job_cmd,
        dag=dag)
    aggregate_db_message_job.set_upstream(wait_for_empty_queue)

The ``pool`` parameter can
be used in conjunction with ``priority_weight`` to define priorities
in the queue, and which tasks get executed first as slots open up in the
pool. The default ``priority_weight`` is ``1``, and can be bumped to any
number. When sorting the queue to evaluate which task should be executed
next, we use the ``priority_weight``, summed up with all of the
``priority_weight`` values from tasks downstream from this task. You can
use this to bump a specific important task and the whole path to that task
gets prioritized accordingly.

Tasks will be scheduled as usual while the slots fill up. Once capacity is
reached, runnable tasks get queued and their state will show as such in the
UI. As slots free up, queued tasks start running based on the
``priority_weight`` (of the task and its descendants).

Note that by default tasks aren't assigned to any pool and their
execution parallelism is only limited to the executor's setting.

Connections
===========

The connection information to external systems is stored in the Airflow
metadata database and managed in the UI (``Menu -> Admin -> Connections``)
A ``conn_id`` is defined there and hostname / login / password / schema
information attached to it. Airflow pipelines can simply refer to the
centrally managed ``conn_id`` without having to hard code any of this
information anywhere.

Many connections with the same ``conn_id`` can be defined and when that
is the case, and when the **hooks** uses the ``get_connection`` method
from ``BaseHook``, Airflow will choose one connection randomly, allowing
for some basic load balancing and fault tolerance when used in conjunction
with retries.

Airflow also has the ability to reference connections via environment
variables from the operating system. The environment variable needs to be
prefixed with ``AIRFLOW_CONN_`` to be considered a connection. When
referencing the connection in the Airflow pipeline, the ``conn_id`` should
be the name of the variable without the prefix. For example, if the ``conn_id``
is named ``postgres_master`` the environment variable should be named
``AIRFLOW_CONN_POSTGRES_MASTER`` (note that the environment variable must be
all uppercase). Airflow assumes the value returned from the environment
variable to be in a URI format (e.g.
``postgres://user:password@localhost:5432/master`` or ``s3://accesskey:secretkey@S3``).

Queues
======

When using the CeleryExecutor, the celery queues that tasks are sent to
can be specified. ``queue`` is an attribute of BaseOperator, so any
task can be assigned to any queue. The default queue for the environment
is defined in the ``airflow.cfg``'s ``celery -> default_queue``. This defines
the queue that tasks get assigned to when not specified, as well as which
queue Airflow workers listen to when started.

Workers can listen to one or multiple queues of tasks. When a worker is
started (using the command ``airflow worker``), a set of comma delimited
queue names can be specified (e.g. ``airflow worker -q spark``). This worker
will then only pick up tasks wired to the specified queue(s).

This can be useful if you need specialized workers, either from a
resource perspective (for say very lightweight tasks where one worker
could take thousands of tasks without a problem), or from an environment
perspective (you want a worker running from within the Spark cluster
itself because it needs a very specific environment and security rights).

XComs
=====

XComs let tasks exchange messages, allowing more nuanced forms of control and
shared state. The name is an abbreviation of "cross-communication". XComs are
principally defined by a key, value, and timestamp, but also track attributes
like the task/DAG that created the XCom and when it should become visible. Any
object that can be pickled can be used as an XCom value, so users should make
sure to use objects of appropriate size.

XComs can be "pushed" (sent) or "pulled" (received). When a task pushes an
XCom, it makes it generally available to other tasks. Tasks can push XComs at
any time by calling the ``xcom_push()`` method. In addition, if a task returns
a value (either from its Operator's ``execute()`` method, or from a
PythonOperator's ``python_callable`` function), then an XCom containing that
value is automatically pushed.

Tasks call ``xcom_pull()`` to retrieve XComs, optionally applying filters
based on criteria like ``key``, source ``task_ids``, and source ``dag_id``. By
default, ``xcom_pull()`` filters for the keys that are automatically given to
XComs when they are pushed by being returned from execute functions (as
opposed to XComs that are pushed manually).

If ``xcom_pull`` is passed a single string for ``task_ids``, then the most
recent XCom value from that task is returned; if a list of ``task_ids`` is
passed, then a correpsonding list of XCom values is returned.

.. code:: python

    # inside a PythonOperator called 'pushing_task'
    def push_function():
        return value

    # inside another PythonOperator where provide_context=True
    def pull_function(**context):
        value = context['task_instance'].xcom_pull(task_ids='pushing_task')

It is also possible to pull XCom directly in a template, here's an example
of what this may look like:

.. code:: sql

    SELECT * FROM {{ task_instance.xcom_pull(task_ids='foo', key='table_name') }}

Note that XComs are similar to `Variables`_, but are specifically designed
for inter-task communication rather than global settings.


Variables
=========

Variables are a generic way to store and retrieve arbitrary content or
settings as a simple key value store within Airflow. Variables can be
listed, created, updated and deleted from the UI (``Admin -> Variables``),
code or CLI. While your pipeline code definition and most of your constants
and variables should be defined in code and stored in source control,
it can be useful to have some variables or configuration items
accessible and modifiable through the UI.


.. code:: python

    from airflow.models import Variable
    foo = Variable.get("foo")
    bar = Variable.get("bar", deserialize_json=True)

The second call assumes ``json`` content and will be deserialized into
``bar``. Note that ``Variable`` is a sqlalchemy model and can be used
as such.


Branching
=========

Sometimes you need a workflow to branch, or only go down a certain path
based on an arbitrary condition which is typically related to something
that happened in an upstream task. One way to do this is by using the
``BranchPythonOperator``.

The ``BranchPythonOperator`` is much like the PythonOperator except that it
expects a python_callable that returns a task_id. The task_id returned
is followed, and all of the other paths are skipped.
The task_id returned by the Python function has to be referencing a task
directly downstream from the BranchPythonOperator task.

Note that using tasks with ``depends_on_past=True`` downstream from
``BranchPythonOperator`` is logically unsound as ``skipped`` status
will invariably lead to block tasks that depend on their past successes.
``skipped`` states propagates where all directly upstream tasks are
``skipped``.

If you want to skip some tasks, keep in mind that you can't have an empty
path, if so make a dummy task.

like this, the dummy task "branch_false" is skipped

.. image:: img/branch_good.png

Not like this, where the join task is skipped

.. image:: img/branch_bad.png

SubDAGs
=======

SubDAGs are perfect for repeating patterns. Defining a function that returns a
DAG object is a nice design pattern when using Airflow.

Airbnb uses the *stage-check-exchange* pattern when loading data. Data is staged
in a temporary table, after which data quality checks are performed against
that table. Once the checks all pass the partition is moved into the production
table.

As another example, consider the following DAG:

.. image:: img/subdag_before.png

We can combine all of the parallel ``task-*`` operators into a single SubDAG,
so that the resulting DAG resembles the following:

.. image:: img/subdag_after.png

Note that SubDAG operators should contain a factory method that returns a DAG
object. This will prevent the SubDAG from being treated like a separate DAG in
the main UI. For example:

.. code:: python

  #dags/subdag.py
  from airflow.models import DAG
  from airflow.operators.dummy_operator import DummyOperator


  # Dag is returned by a factory method
  def sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    dag = DAG(
      '%s.%s' % (parent_dag_name, child_dag_name),
      schedule_interval=schedule_interval,
      start_date=start_date,
    )

    dummy_operator = DummyOperator(
      task_id='dummy_task',
      dag=dag,
    )

    return dag

This SubDAG can then be referenced in your main DAG file:

.. code:: python

  # main_dag.py
  from datetime import datetime, timedelta
  from airflow.models import DAG
  from airflow.operators.subdag_operator import SubDagOperator
  from dags.subdag import sub_dag


  PARENT_DAG_NAME = 'parent_dag'
  CHILD_DAG_NAME = 'child_dag'

  main_dag = DAG(
    dag_id=PARENT_DAG_NAME,
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2016, 1, 1)
  )

  sub_dag = SubDagOperator(
    subdag=sub_dag(PARENT_DAG_NAME, CHILD_DAG_NAME, main_dag.start_date,
                   main_dag.schedule_interval),
    task_id=CHILD_DAG_NAME,
    dag=main_dag,
  )

You can zoom into a SubDagOperator from the graph view of the main DAG to show
the tasks contained within the SubDAG:

.. image:: img/subdag_zoom.png

Some other tips when using SubDAGs:

-  by convention, a SubDAG's ``dag_id`` should be prefixed by its parent and
   a dot. As in ``parent.child``
-  share arguments between the main DAG and the SubDAG by passing arguments to
   the SubDAG operator (as demonstrated above)
-  SubDAGs must have a schedule and be enabled. If the SubDAG's schedule is
   set to ``None`` or ``@once``, the SubDAG will succeed without having done
   anything
-  clearing a SubDagOperator also clears the state of the tasks within
-  marking success on a SubDagOperator does not affect the state of the tasks
   within
-  refrain from using ``depends_on_past=True`` in tasks within the SubDAG as
   this can be confusing
-  it is possible to specify an executor for the SubDAG. It is common to use
   the SequentialExecutor if you want to run the SubDAG in-process and
   effectively limit its parallelism to one. Using LocalExecutor can be
   problematic as it may over-subscribe your worker, running multiple tasks in
   a single slot

See ``airflow/example_dags`` for a demonstration.

SLAs
====

Service Level Agreements, or time by which a task or DAG should have
succeeded, can be set at a task level as a ``timedelta``. If
one or many instances have not succeeded by that time, an alert email is sent
detailing the list of tasks that missed their SLA. The event is also recorded
in the database and made available in the web UI under ``Browse->Missed SLAs``
where events can be analyzed and documented.


Trigger Rules
=============

Though the normal workflow behavior is to trigger tasks when all their
directly upstream tasks have succeeded, Airflow allows for more complex
dependency settings.

All operators have a ``trigger_rule`` argument which defines the rule by which
the generated task get triggered. The default value for ``trigger_rule`` is
``all_success`` and can be defined as "trigger this task when all directly
upstream tasks have succeeded". All other rules described here are based
on direct parent tasks and are values that can be passed to any operator
while creating tasks:

* ``all_success``: (default) all parents have succeeded
* ``all_failed``: all parents are in a ``failed`` or ``upstream_failed`` state
* ``all_done``: all parents are done with their execution
* ``one_failed``: fires as soon as at least one parent has failed, it does not wait for all parents to be done
* ``one_success``: fires as soon as at least one parent succeeds, it does not wait for all parents to be done
* ``dummy``: dependencies are just for show, trigger at will

Note that these can be used in conjunction with ``depends_on_past`` (boolean)
that, when set to ``True``, keeps a task from getting triggered if the
previous schedule for the task hasn't succeeded.


Latest Run Only
===============

Standard workflow behavior involves running a series of tasks for a
particular date/time range. Some workflows, however, perform tasks that
are independent of run time but need to be run on a schedule, much like a
standard cron job. In these cases, backfills or running jobs missed during
a pause just wastes CPU cycles.

For situations like this, you can use the ``LatestOnlyOperator`` to skip
tasks that are not being run during the most recent scheduled run for a
DAG. The ``LatestOnlyOperator`` skips all immediate downstream tasks, and
itself, if the time right now is not between its ``execution_time`` and the
next scheduled ``execution_time``.

One must be aware of the interaction between skipped tasks and trigger
rules. Skipped tasks will cascade through trigger rules ``all_success``
and ``all_failed`` but not ``all_done``, ``one_failed``, ``one_success``,
and ``dummy``. If you would like to use the ``LatestOnlyOperator`` with
trigger rules that do not cascade skips, you will need to ensure that the
``LatestOnlyOperator`` is **directly** upstream of the task you would like
to skip.

It is possible, through use of trigger rules to mix tasks that should run
in the typical date/time dependent mode and those using the
``LatestOnlyOperator``.

For example, consider the following dag:

.. code:: python

  #dags/latest_only_with_trigger.py
  import datetime as dt

  from airflow.models import DAG
  from airflow.operators.dummy_operator import DummyOperator
  from airflow.operators.latest_only_operator import LatestOnlyOperator
  from airflow.utils.trigger_rule import TriggerRule


  dag = DAG(
      dag_id='latest_only_with_trigger',
      schedule_interval=dt.timedelta(hours=4),
      start_date=dt.datetime(2016, 9, 20),
  )

  latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag)

  task1 = DummyOperator(task_id='task1', dag=dag)
  task1.set_upstream(latest_only)

  task2 = DummyOperator(task_id='task2', dag=dag)

  task3 = DummyOperator(task_id='task3', dag=dag)
  task3.set_upstream([task1, task2])

  task4 = DummyOperator(task_id='task4', dag=dag,
                        trigger_rule=TriggerRule.ALL_DONE)
  task4.set_upstream([task1, task2])

In the case of this dag, the ``latest_only`` task will show up as skipped
for all runs except the latest run. ``task1`` is directly downstream of
``latest_only`` and will also skip for all runs except the latest.
``task2`` is entirely independent of ``latest_only`` and will run in all
scheduled periods. ``task3`` is downstream of ``task1`` and ``task2`` and
because of the default ``trigger_rule`` being ``all_success`` will receive
a cascaded skip from ``task1``. ``task4`` is downstream of ``task1`` and
``task2`` but since its ``trigger_rule`` is set to ``all_done`` it will
trigger as soon as ``task1`` has been skipped (a valid completion state)
and ``task2`` has succeeded.

.. image:: img/latest_only_with_trigger.png


Zombies & Undeads
=================

Task instances die all the time, usually as part of their normal life cycle,
but sometimes unexpectedly.

Zombie tasks are characterized by the absence
of an heartbeat (emitted by the job periodically) and a ``running`` status
in the database. They can occur when a worker node can't reach the database,
when Airflow processes are killed externally, or when a node gets rebooted
for instance. Zombie killing is performed periodically by the scheduler's
process.

Undead processes are characterized by the existence of a process and a matching
heartbeat, but Airflow isn't aware of this task as ``running`` in the database.
This mismatch typically occurs as the state of the database is altered,
most likely by deleting rows in the "Task Instances" view in the UI.
Tasks are instructed to verify their state as part of the heartbeat routine,
and terminate themselves upon figuring out that they are in this "undead"
state.


Cluster Policy
==============

Your local airflow settings file can define a ``policy`` function that
has the ability to mutate task attributes based on other task or DAG
attributes. It receives a single argument as a reference to task objects,
and is expected to alter its attributes.

For example, this function could apply a specific queue property when
using a specific operator, or enforce a task timeout policy, making sure
that no tasks run for more than 48 hours. Here's an example of what this
may look like inside your ``airflow_settings.py``:


.. code:: python

    def policy(task):
        if task.__class__.__name__ == 'HivePartitionSensor':
            task.queue = "sensor_queue"
        if task.timeout > timedelta(hours=48):
            task.timeout = timedelta(hours=48)


Documentation & Notes
=====================

It's possible to add documentation or notes to your dags & task objects that
become visible in the web interface ("Graph View" for dags, "Task Details" for
tasks). There are a set of special task attributes that get rendered as rich
content if defined:

==========  ================
attribute   rendered to
==========  ================
doc         monospace
doc_json    json
doc_yaml    yaml
doc_md      markdown
doc_rst     reStructuredText
==========  ================

Please note that for dags, dag_md is the only attribute interpreted.

This is especially useful if your tasks are built dynamically from
configuration files, it allows you to expose the configuration that led
to the related tasks in Airflow.

.. code:: python

    """
    ### My great DAG
    """

    dag = DAG('my_dag', default_args=default_args)
    dag.doc_md = __doc__

    t = BashOperator("foo", dag=dag)
    t.doc_md = """\
    #Title"
    Here's a [url](www.airbnb.com)
    """

This content will get rendered as markdown respectively in the "Graph View" and
"Task Details" pages.

.. _jinja-templating:

Jinja Templating
================

Airflow leverages the power of
`Jinja Templating <http://jinja.pocoo.org/docs/dev/>`_ and this can be a
powerful tool to use in combination with macros (see the :ref:`macros` section).

For example, say you want to pass the execution date as an environment variable
to a Bash script using the ``BashOperator``.

.. code:: python

  # The execution date as YYYY-MM-DD
  date = "{{ ds }}"
  t = BashOperator(
      task_id='test_env',
      bash_command='/tmp/test.sh ',
      dag=dag,
      env={'EXECUTION_DATE': date})

Here, ``{{ ds }}`` is a macro, and because the ``env`` parameter of the
``BashOperator`` is templated with Jinja, the execution date will be available
as an environment variable named ``EXECUTION_DATE`` in your Bash script.

You can use Jinja templating with every parameter that is marked as "templated"
in the documentation. Template substitution occurs just before the pre_execute
function of your operator is called.

Packaged dags
'''''''''''''
While often you will specify dags in a single ``.py`` file it might sometimes
be required to combine dag and its dependencies. For example, you might want
to combine several dags together to version them together or you might want
to manage them together or you might need an extra module that is not available
by default on the system you are running airflow on. To allow this you can create
a zip file that contains the dag(s) in the root of the zip file and have the extra
modules unpacked in directories.

For instance you can create a zip file that looks like this:

.. code-block:: bash

    my_dag1.py
    my_dag2.py
    package1/__init__.py
    package1/functions.py

Airflow will scan the zip file and try to load ``my_dag1.py`` and ``my_dag2.py``.
It will not go into subdirectories as these are considered to be potential
packages.

In case you would like to add module dependencies to your DAG you basically would
do the same, but then it is more to use a virtualenv and pip.

.. code-block:: bash

    virtualenv zip_dag
    source zip_dag/bin/activate

    mkdir zip_dag_contents
    cd zip_dag_contents

    pip install --install-option="--install-lib=$PWD" my_useful_package
    cp ~/my_dag.py .

    zip -r zip_dag.zip *

.. note:: the zip file will be inserted at the beginning of module search list
   (sys.path) and as such it will be available to any other code that resides
   within the same interpreter.

.. note:: packaged dags cannot be used with pickling turned on.

.. note:: packaged dags cannot contain dynamic libraries (eg. libz.so) these need
   to be available on the system if a module needs those. In other words only
   pure python modules can be packaged.

