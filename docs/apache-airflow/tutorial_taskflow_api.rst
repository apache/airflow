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




Tutorial on the TaskFlow API
============================

This tutorial builds on the regular Airflow Tutorial and focuses specifically
on writing data pipelines using the TaskFlow API paradigm which is introduced as
part of Airflow 2.0 and contrasts this with DAGs written using the traditional paradigm.

The data pipeline chosen here is a simple ETL pattern with
three separate tasks for Extract, Transform, and Load.

Example "TaskFlow API" ETL Pipeline
-----------------------------------

Here is very simple ETL pipeline using the TaskFlow API paradigm. A more detailed
explanation is given below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl.py
    :language: python
    :start-after: [START tutorial]
    :end-before: [END tutorial]

It's a DAG definition file
--------------------------

If this is the first DAG file you are looking at, please note that this Python script
is interpreted by Airflow and is a configuration file for your data pipeline.
For a complete introduction to DAG files, please look at the core :doc:`Airflow tutorial<tutorial>`
which covers DAG structure and definitions extensively.


Instantiate a DAG
-----------------

We are creating a DAG which is the collection of our tasks with dependencies between
the tasks. This is a very simple definition, since we just want the DAG to be run
when we set this up with Airflow, without any retries or complex scheduling.
In this example, please notice that we are creating this DAG using the ``@dag`` decorator
as shown below, with the python function name acting as the DAG identifier.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl.py
    :language: python
    :start-after: [START instantiate_dag]
    :end-before: [END instantiate_dag]

Tasks
-----
In this data pipeline, tasks are created based on Python functions using the ``@task`` decorator
as shown below. The function name acts as a unique identifier for the task.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl.py
    :language: python
    :dedent: 4
    :start-after: [START extract]
    :end-before: [END extract]

The returned value, which in this case is a dictionary, will be made available for use in later tasks.

The Transform and Load tasks are created in the same manner as the Extract task shown above.

Main flow of the DAG
--------------------
Now that we have the Extract, Transform, and Load tasks defined based on the Python functions,
we can move to the main part of the DAG.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl.py
    :language: python
    :dedent: 4
    :start-after: [START main_flow]
    :end-before: [END main_flow]

That's it, we are done!
We have invoked the Extract task, obtained the order data from there and sent it over to
the Transform task for summarization, and then invoked the Load task with the summarized data.
The dependencies between the tasks and the passing of data between these tasks which could be
running on different workers on different nodes on the network is all handled by Airflow.

Now to actually enable this to be run as a DAG, we invoke the python function
``tutorial_taskflow_api_etl`` set up using the ``@dag`` decorator earlier, as shown below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl.py
    :language: python
    :start-after: [START dag_invocation]
    :end-before: [END dag_invocation]


But how?
--------
For experienced Airflow DAG authors, this is startlingly simple! Let's contrast this with
how this DAG had to be written before Airflow 2.0 below:

.. exampleinclude:: /../../airflow/example_dags/tutorial_etl_dag.py
    :language: python
    :start-after: [START tutorial]
    :end-before: [END tutorial]

All of the processing shown above is being done in the new Airflow 2.0 dag as well, but
it is all abstracted from the DAG developer.

Let's examine this in detail by looking at the Transform task in isolation since it is
in the middle of the data pipeline. In Airflow 1.x, this task is defined as shown below:

.. exampleinclude:: /../../airflow/example_dags/tutorial_etl_dag.py
    :language: python
    :dedent: 4
    :start-after: [START transform_function]
    :end-before: [END transform_function]

As we see here, the data being processed in the Transform function is passed to it using Xcom
variables. In turn, the summarized data from the Transform function is also placed
into another Xcom variable which will then be used by the Load task.

Contrasting that with TaskFlow API in Airflow 2.0 as shown below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl.py
    :language: python
    :dedent: 4
    :start-after: [START transform]
    :end-before: [END transform]

All of the Xcom usage for data passing between these tasks is abstracted away from the DAG author
in Airflow 2.0. However, Xcom variables are used behind the scenes and can be viewed using
the Airflow UI as necessary for debugging or DAG monitoring.

Similarly, task dependencies are automatically generated within TaskFlows based on the
functional invocation of tasks. In Airflow 1.x, tasks had to be explicitly created and
dependencies specified as shown below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_etl_dag.py
    :language: python
    :dedent: 4
    :start-after: [START main_flow]
    :end-before: [END main_flow]

In contrast, with the TaskFlow API in Airflow 2.0, the invocation itself automatically generates
the dependencies as shown below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl.py
    :language: python
    :dedent: 4
    :start-after: [START main_flow]
    :end-before: [END main_flow]

Using the TaskFlow API with Virtual Environments
----------------------------------------------------------

As of Airflow 2.0.3, you will have the ability to use the TaskFlow API with a
virtual environment. This added functionality will allow a much more
comprehensive range of use-cases for the TaskFlow API, as you will not be limited to the
packages and system libraries of the Airflow worker.

To run your Airflow task in a virtual environment, switch your ``@task`` decorator to a ``@task.virtualenv``
decorator. The ``@task.virtualenv`` decorator will allow you to create a new virtualenv with custom libraries
and even a different python version to run your function.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl_virtualenv.py
    :language: python
    :dedent: 4
    :start-after: [START extract_virtualenv]
    :end-before: [END extract_virtualenv]

This option should allow for far greater flexibility for users who wish to keep their workflows more simple
and pythonic.

Using the Taskflow API with Docker or Virtual Environments
----------------------------------------------------------

As of Airflow 2.2, you will have the ability to use the Taskflow API with either a
Docker container or Python virtual environment. This added functionality will allow a much more
comprehensive range of use-cases for the Taskflow API, as you will not be limited to the
packages and system libraries of the Airflow worker.

To use a docker image with the Taskflow API, change the decorator to ``@task.docker``
and add any needed arguments to correctly run the task. Please note that the docker
image must have a working Python installed and take in a bash command as the ``command`` argument.

Below is an example of using the ``@task.docker`` decorator to run a python task.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl_docker_virtualenv.py
    :language: python
    :dedent: 4
    :start-after: [START transform_docker]
    :end-before: [END transform_docker]

If you don't want to run your image on a Docker environment, and instead want to create a separate virtual
environment on the same machine, you can use the ``@task.virtualenv`` decorator instead. The ``@task.virtualenv``
decorator will allow you to create a new virtualenv with custom libraries and even a different
Python version to run your function.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl_docker_virtualenv.py
    :language: python
    :dedent: 4
    :start-after: [START extract_virtualenv]
    :end-before: [END extract_virtualenv]

These two options should allow for far greater flexibility for users who wish to keep their workflows more simple
and pythonic.

Creating Custom TaskFlow Decorators
-----------------------------------

As of Airflow 2.2, users can now integrate custom decorators into their provider packages and have those decorators
appear natively as part of the ``@task.____`` design.

For an example. Let's say you were trying to create a "foo" decorator. To create ``@task.foo``, follow the following
steps:

1. Create a ``FooDecoratedOperator``

In this case, we are assuming that you have a ``FooOperator`` that takes a python function as an argument.
By creating a ``FooDecoratedOperator`` that inherits from ``FooOperator`` and
``airflow.decorators.base.DecoratedOperator``, Airflow will supply much of the needed functionality required to treat
your new class as a taskflow native class.

2. Create a ``foo_task`` function

Once you have your decorated class, create a function that takes arguments ``python_callable``\, ``multiple_outputs``\,
and ``kwargs``\. This function will use the ``airflow.decorators.base.task_decorator_factory`` function to convert
the new ``FooDecoratedOperator`` into a TaskFlow function decorator!

.. code-block:: python

   def foo_task(
       python_callable: Optional[Callable] = None,
       multiple_outputs: Optional[bool] = None,
       **kwargs
   ):
       return task_decorator_factory(
           python_callable=python_callable,
           multiple_outputs=multiple_outputs,
           decorated_operator_class=FooDecoratedOperator,
           **kwargs,
       )

3. Register your new decorator in the provider.yaml of your provider

Finally, add a key-value of ``decorator-name``:``path-to-function`` to your provider.yaml. When Airflow starts, the
``ProviderManager`` class will automatically import this value and ``task.decorator-name`` will work as a new
decorator!

.. code-block:: yaml

   package-name: apache-airflow-providers-docker
   name: Docker
   description: |
       `Docker <https://docs.docker.com/install/>`__

   task-decorators:
       docker: airflow.providers.docker.operators.docker.docker_decorator


4. (Optional) Create a Mixin class so that your decorator will show up in your IDE's autocomplete

For better or worse, Python IDEs can not autocomplete dynamically
generated methods (see `here <https://intellij-support.jetbrains.com/hc/en-us/community/posts/115000665110-auto-completion-for-dynamic-module-attributes-in-python>`_).

To get around this, we had to find a solution that was "best possible." IDEs will only allow typing
through stub files, but we wanted to avoid any situation where a user would update their provider and the autocomplete
would be out of sync with the provider's actual parameters.

To hack around this problem, we found that you could extend the ``_TaskDecorator`` class in the ``__init__.pyi`` file
and the correct autocomplete will show up in the IDE.

To correctly implement this, please take the following steps:

Create a ``Mixin`` class for your decorator

Mixin classes are classes in python that tell the python interpreter that python can import them at any time.
Because they are not dependent on other classes, Mixin classes are great for multiple inheritance.

In the DockerDecorator we created a Mixin class that looks like this

.. exampleinclude:: ../apache-airflow/docker_decorator.py
    :language: python
    :dedent: 4
    :start-after: [START decoratormixin]
    :end-before: [END decoratormixin]

Notice that the function does not actually need to return anything. We will only use this class for type checking.

Once you have your Mixin class ready, go to ``airflow.decorators.__init__.pyi`` and add section similar to this

.. exampleinclude:: ../../airflow/decorators/__init__.pyi
    :language: python
    :dedent: 4
    :start-after: [START import_docker]
    :end-before: [END import_docker]


This statement will prevent Airflow from failing if your provider does not exist.

Then at the bottom add a section to import your Mixin class

.. exampleinclude:: ../../airflow/decorators/__init__.pyi
    :language: python
    :dedent: 4
    :start-after: [START extend_docker]
    :end-before: [END extend_docker]

Now once the next Airflow minor release comes out, users will be able to see your decorator in IDE autocomplete. This autocomplete will change based on the version of the provider that the user downloads.

Please note that this step is not required to create a working decorator but does create a better experience for developers.


Multiple outputs inference
--------------------------
Tasks can also infer multiple outputs by using dict python typing.

.. code-block:: python

   @task
   def identity_dict(x: int, y: int) -> Dict[str, int]:
       return {"x": x, "y": y}

By using the typing ``Dict`` for the function return type, the ``multiple_outputs`` parameter
is automatically set to true.

Note, If you manually set the ``multiple_outputs`` parameter the inference is disabled and
the parameter value is used.

Adding dependencies to decorated tasks from regular tasks
---------------------------------------------------------
The above tutorial shows how to create dependencies between python-based tasks. However, it is
quite possible while writing a DAG to have some pre-existing tasks such as :class:`~airflow.operators.bash.BashOperator` or :class:`~airflow.sensors.filesystem.FileSensor`
based tasks which need to be run first before a python-based task is run.

Building this dependency is shown in the code below:

.. code-block:: python

    @task()
    def extract_from_file():
        """
        #### Extract from file task
        A simple Extract task to get data ready for the rest of the data
        pipeline, by reading the data from a file into a pandas dataframe
        """
        order_data_file = "/tmp/order_data.csv"
        order_data_df = pd.read_csv(order_data_file)


    file_task = FileSensor(task_id="check_file", filepath="/tmp/order_data.csv")
    order_data = extract_from_file()

    file_task >> order_data


In the above code block, a new python-based task is defined as ``extract_from_file`` which
reads the data from a known file location.
In the main DAG, a new ``FileSensor`` task is defined to check for this file. Please note
that this is a Sensor task which waits for the file.
Finally, a dependency between this Sensor task and the python-based task is specified.


Consuming XCOMs with decorated tasks from regular tasks
---------------------------------------------------------
You may additionally find it necessary to consume an XCOM from a pre-existing task as an input into python-based tasks.

Building this dependency is shown in the code below:

.. code-block:: python

    get_api_results_task = SimpleHttpOperator(
        task_id="get_api_results",
        endpoint="/api/query",
        do_xcom_push=True,
        http_conn_id="http",
    )


    @task(max_retries=2)
    def parse_results(api_results):
        return json.loads(api_results)


    parsed_results = parsed_results(get_api_results_task.output)


In the above code block, a :class:`~airflow.providers.http.operators.http.SimpleHttpOperator` result
was captured via :doc:`XCOMs </concepts/xcoms>`. This XCOM result, which is the task output, was then passed
to a TaskFlow decorated task which parses the response as JSON - and the rest continues as expected.


What's Next?
------------

You have seen how simple it is to write DAGs using the TaskFlow API paradigm within Airflow 2.0. Please do
read the :doc:`Concepts section </concepts/index>` for detailed explanation of Airflow concepts such as DAGs, Tasks,
Operators, and more. There's also a whole section on the :doc:`TaskFlow API </concepts/taskflow>` and the ``@task`` decorator.
