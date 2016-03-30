Concepts
========

Operators
'''''''''

Operators allow for generating a certain type of task on the graph. There
are 3 main type of operators:

-  **Sensor:** Waits for events to happen. This could be a file appearing
   in HDFS, the existence of a Hive partition, or waiting for an arbitrary
   MySQL query to return a row.
-  **Remote Execution:** Triggers an operation in a remote system. This
   could be an HQL statement in Hive, a Pig script, a map reduce job, a
   stored procedure in Oracle or a Bash script to run.
-  **Data transfers:** Move data from one system to another. Push data
   from Hive to MySQL, from a local file to HDFS, from Postgres to
   Oracle, or anything of that nature.

Tasks
'''''

A task represents the instantiation of an operator and becomes a node in
the directed acyclic graph (DAG). The instantiation defines specific
values when calling the abstract operator. A task could be waiting for a
specific partition in Hive, or triggering a specific DML statement in
Oracle.

Task Instances
''''''''''''''

A task instance represents a task run, for a specific point in time.
While the task defines a start datetime and a schedule (say every hour
or every day), a task instance represents a specific run of a task. A
task instance will have a status of either "started", "retrying",
"failed" or "success"

Hooks
'''''

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
'''''

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
'''''''''''

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
is named ``POSTGRES_MASTER`` the environment variable should be named
``AIRFLOW_CONN_POSTGRES_MASTER``. Airflow assumes the value returned
from the environment variable to be in a URI format
(e.g. ``postgres://user:password@localhost:5432/master``).

Queues
''''''

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
'''''

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
'''''''''

Variables are a generic way to store and retrieve arbitrary content or
settings as a simple key value store within Airflow. Variables can be
listed, created, updated and deleted from the UI (``Admin -> Variables``)
or from code. While your pipeline code definition and most of your constants
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
'''''''''

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
'''''''

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
  from airflow.operators import DummyOperator


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
  from airflow.operators import SubDagOperator
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
''''

Service Level Agreements, or time by which a task or DAG should have
succeeded, can be set at a task level as a ``timedelta``. If
one or many instances have not succeeded by that time, an alert email is sent
detailing the list of tasks that missed their SLA. The event is also recorded
in the database and made available in the web UI under ``Browse->Missed SLAs``
where events can be analyzed and documented.


Trigger Rules
'''''''''''''

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


Zombies & Undeads
'''''''''''''''''

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
''''''''''''''

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


Task Documentation & Notes
''''''''''''''''''''''''''
It's possible to add documentation or notes to your task objects that become
visible in the "Task Details" view in the web interface. There are a set
of special task attributes that get rendered as rich content if defined:

==========  ================
attribute   rendered to
==========  ================
doc         monospace
doc_json    json
doc_yaml    yaml
doc_md      markdown
doc_rst     reStructuredText
==========  ================

This is especially useful if your tasks are built dynamically from
configuration files, it allows you to expose the configuration that led
to the related tasks in Airflow.

.. code:: python
    
    t = BashOperator("foo", dag=dag)
    t.doc_md = """\
    #Title"
    Here's a [url](www.airbnb.com)
    """

This content will get rendered as markdown in the "Task Details" page.

