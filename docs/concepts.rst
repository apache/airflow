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
the ``airflow.connection.Connection`` model to retrieve hostnames
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
metadata database and managed in the UI (``Menu -> Admin -> Connections``).
A ``conn_id`` is defined there and hostname / login / password / schema
information attached to it. Airflow pipelines can simply refer to the
centrally managed ``conn_id`` without having to hard code any of this
information anywhere.

Many connections with the same ``conn_id`` can be defined and when that
is the case, and when the **hooks** uses the ``get_connection`` method
from ``BaseHook``, Airflow will choose one connection randomly, allowing
for some basic load balancing and fault tolerance when used in conjunction
with retries.

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

    # inside another PythonOperator
    def pull_function(**context):
        value = context['ti'].xcom_pull(task_ids='pushing_task')


XComs are similar to Variables, but are specifically designed for inter-task
communication rather than global settings.


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


SLAs
''''

Service License Agreements, or time by which a task or DAG should have
succeeded, can be set at a task level as a ``timedelta``. If
one or many instances have not succeeded by that time, an alert email is sent
detailing the list of tasks that missed their SLA. The event is also recorded
in the database and made available in the web UI under ``Browse->Missed SLAs``
where events can be analyzed and documented.

