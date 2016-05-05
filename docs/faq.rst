FAQ
========

**Why isn't my task getting scheduled?**

There are very many reasons why your task might not be getting scheduled.
Here are some of the common causes:

- Does your script "compile", can the Airflow engine parse it and find your
  DAG object. To test this, you can run ``airflow list_dags`` and
  confirm that your DAG shows up in the list. You can also run
  ``airflow list_tasks foo_dag_id --tree`` and confirm that your task
  shows up in the list as expected. If you use the CeleryExecutor, you
  may way to confirm that this works both where the scheduler runs as well
  as where the worker runs.

- Is your ``start_date`` set properly? The Airflow scheduler triggers the
  task soon after the ``start_date + scheduler_interval`` is passed.

- Is your ``start_date`` beyond where you can see it in the UI? If you
  set your it to some time say 3 months ago, you won't be able to see
  it in the main view in the UI, but you should be able to see it in the
  ``Menu -> Browse ->Task Instances``.

- Are the dependencies for the task met. The task instances directly
  upstream from the task need to be in a ``success`` state. Also,
  if you have set ``depends_on_past=True``, the previous task instance
  needs to have succeeded (except if it is the first run for that task).
  Also, if ``wait_for_downstream=True``, make sure you understand
  what it means.
  You can view how these properties are set from the ``Task Details``
  page for your task.

- Are the DagRuns you need created and active? A DagRun represents a specific
  execution of an entire DAG and has a state (running, success, failed, ...).
  The scheduler creates new DagRun as it moves forward, but never goes back
  in time to create new ones. The scheduler only evaluates ``running`` DagRuns
  to see what task instances it can trigger. Note that clearing tasks
  instances (from the UI or CLI) does set the state of a DagRun back to
  running. You can bulk view the list of DagRuns and alter states by clicking
  on the schedule tag for a DAG.

- Is the ``concurrency`` parameter of your DAG reached? ``concurency`` defines
  how many ``running`` task instances a DAG is allowed to have, beyond which
  point things get queued.

- Is the ``max_active_runs`` parameter of your DAG reached? ``max_active_runs`` defines
  how many ``running`` concurrent instances of a DAG there are allowed to be.

You may also want to read the Scheduler section of the docs and make
sure you fully understand how it proceeds.


**How do I trigger tasks based on another task's failure?**

Check out the ``Trigger Rule`` section in the Concepts section of the
documentation

**Why are connection passwords still not encrypted in the metadata db after I installed airflow[crypto]**?

- Verify that the ``fernet_key`` defined in ``$AIRFLOW_HOME/airflow.cfg`` is a valid Fernet key. It must be a base64-encoded 32-byte key. You need to restart the webserver after you update the key
- For existing connections (the ones that you had defined before installing ``airflow[crypto]`` and creating a Fernet key), you need to open each connection in the connection admin UI, re-type the password, and save it

**What's the deal with ``start_date``?**

``start_date`` is partly legacy from the pre-DagRun era, but it is still
relevant in many ways. When creating a new DAG, you probably want to set
a global ``start_date`` for your tasks using ``default_args``. The first
DagRun to be created will be based on the ``min(start_date)`` for all your
task. From that point on, the scheduler creates new DagRuns based on
your ``schedule_interval`` and the corresponding task instances run as your
dependencies are met. When introducing new tasks to your DAG, you need to
pay special attention to ``start_date``, and may want to reactivate
inactive DagRuns to get the new task to get onboarded properly.

We recommend against using dynamic values as ``start_date``, especially
``datetime.now()`` as it can be quite confusing. The task is triggered
once the period closes, and in theory an ``@hourly`` DAG would never get to
an hour after now as ``now()`` moves along.

We also recommend using rounded ``start_date`` in relation to your
``schedule_interval``. This means an ``@hourly`` would be at ``00:00``
minutes:seconds, a ``@daily`` job at midnight, a ``@monthly`` job on the
first of the month. You can use any sensor or a ``TimeDeltaSensor`` to delay
the execution of tasks within that period. While ``schedule_interval``
does allow specifying a ``datetime.timedelta``
object, we recommend using the macros or cron expressions instead, as
it enforces this idea of rounded schedules.

When using ``depends_on_past=True`` it's important to pay special attention
to ``start_date`` as the past dependency is not enforced only on the specific
schedule of the ``start_date`` specified for the task. It' also
important to watch DagRun activity status in time when introducing
new ``depends_on_past=True``, unless you are planning on running a backfill
for the new task(s).

Also important to note is that the tasks ``start_date``, in the context of a
backfill CLI command, get overridden by the backfill's command ``start_date``.
This allows for a backfill on tasks that have ``depends_on_past=True`` to
actually start, if it wasn't the case, the backfill just wouldn't start.
