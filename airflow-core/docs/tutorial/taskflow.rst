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


Pythonic Dags with the TaskFlow API
===================================

In the first tutorial, you built your first Airflow Dag using traditional Operators like ``BashOperator``.
Now let's look at a more modern and Pythonic way to write workflows using the **TaskFlow API** — introduced in Airflow
2.0.

The TaskFlow API is designed to make your code simpler, cleaner, and easier to maintain. You write plain Python
functions, decorate them, and Airflow handles the rest — including task creation, dependency wiring, and passing data
between tasks.

In this tutorial, we'll create a simple ETL pipeline — Extract → Transform → Load using the TaskFlow API.
Let's dive in!

The Big Picture: A TaskFlow Pipeline
------------------------------------

Here's what the full pipeline looks like using TaskFlow. Don't worry if some of it looks unfamiliar — we'll break it
down step-by-step.

.. exampleinclude:: /../src/airflow/example_dags/tutorial_taskflow_api.py
    :language: python
    :start-after: [START tutorial]
    :end-before: [END tutorial]

Step 1: Define the Dag
----------------------

Just like before, your Dag is a Python script that Airflow loads and parses. But this time, we're using the ``@dag``
decorator to define it.

.. exampleinclude:: /../src/airflow/example_dags/tutorial_taskflow_api.py
    :language: python
    :start-after: [START instantiate_dag]
    :end-before: [END instantiate_dag]

|

To make this Dag discoverable by Airflow, we can call the Python function that was decorated with ``@dag``:

.. exampleinclude:: /../src/airflow/example_dags/tutorial_taskflow_api.py
    :language: python
    :start-after: [START dag_invocation]
    :end-before: [END dag_invocation]

|

.. versionchanged:: 2.4
  If you're using the ``@dag`` decorator or defining your Dag in a ``with`` block, you no longer need to assign it to a
  global variable. Airflow will find it automatically.

You can visualize your Dag in the Airflow UI! Once your Dag is loaded, navigate to the Graph View to see how tasks are
connected.

Step 2: Write Your Tasks with ``@task``
---------------------------------------

With TaskFlow, each task is just a regular Python function. You can use the ``@task`` decorator to turn it into a task
that Airflow can schedule and run. Here's the ``extract`` task:

.. exampleinclude:: /../src/airflow/example_dags/tutorial_taskflow_api.py
    :language: python
    :dedent: 4
    :start-after: [START extract]
    :end-before: [END extract]

|

The function's return value is passed to the next task — no manual use of ``XComs`` required. Under the hood, TaskFlow
uses ``XComs`` to manage data passing automatically, abstracting away the complexity of manual XCom management from the
previous methods. You'll define ``transform`` and ``load`` tasks using the same pattern.

Notice the use of ``@task(multiple_outputs=True)`` above — this tells Airflow that the function returns a dictionary of
values that should be split into individual XComs. Each key in the returned dictionary becomes its own XCom entry, which
makes it easy to reference specific values in downstream tasks. If you omit ``multiple_outputs=True``, the entire
dictionary is stored as a single XCom instead, and must be accessed as a whole.

Step 3: Build the Flow
----------------------

Once the tasks are defined, you can build the pipeline by simply calling them like Python functions. Airflow uses this
functional invocation to set task dependencies and manage data passing.

.. exampleinclude:: /../src/airflow/example_dags/tutorial_taskflow_api.py
    :language: python
    :dedent: 4
    :start-after: [START main_flow]
    :end-before: [END main_flow]

|

That's it! Airflow knows how to schedule and orchestrate your pipeline from this code alone.

Running Your Dag
----------------

To enable and trigger your Dag:

1. Navigate to the Airflow UI.
2. Find your Dag in the list and click the toggle to enable it.
3. You can trigger it manually by clicking the "Trigger Dag" button, or wait for it to run on its schedule.

What's Happening Behind the Scenes?
-----------------------------------

If you've used Airflow 1.x, this probably feels like magic. Let's compare what's happening under the hood.

The "Old Way": Manual Wiring and XComs
''''''''''''''''''''''''''''''''''''''

Before the TaskFlow API, you had to use Operators like ``PythonOperator`` and pass data manually between tasks using
``XComs``.

Here's what the same Dag might have looked like using the traditional approach:

.. code-block:: python

   import json
   import pendulum
   from airflow.sdk import DAG
   from airflow.providers.standard.operators.python import PythonOperator


   def extract():
       # Old way: simulate extracting data from a JSON string
       data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
       return json.loads(data_string)


   def transform(ti):
       # Old way: manually pull from XCom
       order_data_dict = ti.xcom_pull(task_ids="extract")
       total_order_value = sum(order_data_dict.values())
       return {"total_order_value": total_order_value}


   def load(ti):
       # Old way: manually pull from XCom
       total = ti.xcom_pull(task_ids="transform")["total_order_value"]
       print(f"Total order value is: {total:.2f}")


   with DAG(
       dag_id="legacy_etl_pipeline",
       schedule=None,
       start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
       catchup=False,
       tags=["example"],
   ) as dag:
       extract_task = PythonOperator(task_id="extract", python_callable=extract)
       transform_task = PythonOperator(task_id="transform", python_callable=transform)
       load_task = PythonOperator(task_id="load", python_callable=load)

       extract_task >> transform_task >> load_task

.. note::
   This version produces the same result as the TaskFlow API example, but requires explicit management of ``XComs`` and task dependencies.

The TaskFlow Way
''''''''''''''''

Using TaskFlow, all of this is handled automatically.

.. exampleinclude:: /../src/airflow/example_dags/tutorial_taskflow_api.py
    :language: python
    :start-after: [START tutorial]
    :end-before: [END tutorial]

|

Airflow still uses ``XComs`` and builds a dependency graph — it's just abstracted away so you can focus on your business
logic.

How XComs Work
--------------

TaskFlow return values are stored as ``XComs`` automatically. These values can be inspected in the UI under the "XCom" tab.
Manual ``xcom_pull()`` is still possible for traditional operators.


Error Handling and Retries
---------------------------

You can easily configure retries for your tasks using decorators. For example, you can set a maximum number of retries
directly in the task decorator:

.. code-block:: python

    @task(retries=3)
    def my_task(): ...

This helps ensure that transient failures do not lead to task failure.

Task Parameterization
---------------------

You can reuse decorated tasks in multiple Dags and override parameters like ``task_id`` or ``retries``.

.. code-block:: python

    start = add_task.override(task_id="start")(1, 2)

|

You can even import decorated tasks from a shared module.

What to Explore Next
--------------------

Nice work! You've now written your first pipeline using the TaskFlow API. Curious where to go from here?

- Add a new task to the Dag -- maybe a filter or validation step
- Modify return values and pass multiple outputs
- Explore retries and overrides with ``.override(task_id="...")``
- Open the Airflow UI and inspect how the data flows between tasks, including task logs and dependencies

.. seealso::

   - Continue to the next step: :doc:`/tutorial/pipeline`
   - Learn more in the :doc:`TaskFlow API docs </core-concepts/taskflow>` or continue below for :ref:`advanced-taskflow-patterns`
   - Read about Airflow concepts in :doc:`/core-concepts/index`

.. _advanced-taskflow-patterns:

Advanced TaskFlow Patterns
--------------------------

Once you're comfortable with the basics, here are a few powerful techniques you can try.

Reusing Decorated Tasks
'''''''''''''''''''''''

You can reuse decorated tasks across multiple Dags or Dag runs. This is especially useful for common logic like reusable
utilities or shared business rules. Use ``.override()`` to customize task metadata like ``task_id`` or ``retries``.

.. code-block:: python

    start = add_task.override(task_id="start")(1, 2)

You can even import decorated tasks from a shared module.

Handling Conflicting Dependencies
'''''''''''''''''''''''''''''''''

Sometimes tasks require different Python dependencies than the rest of your Dag — for example, specialized libraries or
system-level packages. TaskFlow supports multiple execution environments to isolate those dependencies.

.. _taskflow-dynamically-created-virtualenv:

**Dynamically Created Virtualenv**

Creates a temporary virtualenv at task runtime. Great for experimental or dynamic tasks, but may have cold start
overhead.

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags//example_python_decorator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_python_venv]
    :end-before: [END howto_operator_python_venv]

|

.. _taskflow-external-python-environment:

**External Python Environment**

Executes the task using a pre-installed Python interpreter — ideal for consistent environments or shared virtualenvs.

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags//example_python_decorator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_external_python]
    :end-before: [END howto_operator_external_python]

|

.. _taskflow-docker_environment:

**Docker Environment**

Runs your task in a Docker container. Useful for packaging everything the task needs — but requires Docker to be
available on your worker.

.. exampleinclude:: /../../providers/docker/tests/system/docker/example_taskflow_api_docker_virtualenv.py
    :language: python
    :dedent: 4
    :start-after: [START transform_docker]
    :end-before: [END transform_docker]

|

.. note:: Requires Airflow 2.2 and the Docker provider.

.. _tasfklow-kpo:

**KubernetesPodOperator**

Runs your task inside a Kubernetes pod, fully isolated from the main Airflow environment. Ideal for large tasks or tasks
requiring custom runtimes.

.. exampleinclude:: /../../providers/cncf/kubernetes/tests/system/cncf/kubernetes/example_kubernetes_decorator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_kubernetes]
    :end-before: [END howto_operator_kubernetes]

|

.. note:: Requires Airflow 2.4 and the Kubernetes provider.

.. _taskflow-using-sensors:

Using Sensors
'''''''''''''

Use ``@task.sensor`` to build lightweight, reusable sensors using Python functions. These support both poke and reschedule
modes.

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags//example_sensor_decorator.py
    :language: python
    :start-after: [START tutorial]
    :end-before: [END tutorial]

Mixing with Traditional Tasks
'''''''''''''''''''''''''''''

You can combine decorated tasks with classic Operators. This is helpful when using community providers or when migrating
incrementally to TaskFlow.

You can chain TaskFlow and traditional tasks using ``>>`` or pass data using the ``.output`` attribute.

.. _taskflow/accessing_context_variables:

Templating in TaskFlow
''''''''''''''''''''''

Like traditional tasks, decorated TaskFlow functions support templated arguments — including loading content from files
or using runtime parameters.

When running your callable, Airflow will pass a set of keyword arguments that
can be used in your function. This set of kwargs correspond exactly to what you
can use in your Jinja templates. For this to work, you can add context keys you
would like to receive in the function as keyword arguments.

For example, the callable in the code block below will get values of the ``ti``
and ``next_ds`` context variables:

.. code-block:: python

   @task
   def my_python_callable(*, ti, next_ds):
       pass


You can also choose to receive the entire context with ``**kwargs``. Note that
this can incur a slight performance penalty since Airflow will need to
expand the entire context that likely contains many things you don't actually
need. It is therefore more recommended for you to use explicit arguments, as
demonstrated in the previous paragraph.

.. code-block:: python

   @task
   def my_python_callable(**kwargs):
       ti = kwargs["ti"]
       next_ds = kwargs["next_ds"]

Also, sometimes you might want to access the context somewhere deep in the stack, but you do not want to pass
the context variables from the task callable. You can still access execution context via the ``get_current_context``
method.

.. code-block:: python

    from airflow.sdk import get_current_context


    def some_function_in_your_library():
        context = get_current_context()
        ti = context["ti"]


Arguments passed to decorated functions are automatically templated. You can also template file using
``templates_exts``:

.. code-block:: python

    @task(templates_exts=[".sql"])
    def read_sql(sql): ...


Conditional Execution
'''''''''''''''''''''

Use ``@task.run_if()`` or ``@task.skip_if()`` to control whether a task runs based on dynamic conditions at runtime —
without altering your Dag structure.

.. code-block:: python

    @task.run_if(lambda ctx: ctx["task_instance"].task_id == "run")
    @task.bash()
    def echo():
        return "echo 'run'"

What's Next
-----------

Now that you've seen how to build clean, maintainable Dags using the TaskFlow API, here are some good next steps:

- Explore asset-aware workflows in :doc:`/authoring-and-scheduling/asset-scheduling`
- Dive into scheduling patterns in :ref:`Scheduling Options <scheduling-section>`
- Move to the next tutorial: :doc:`/tutorial/pipeline`
