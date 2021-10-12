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

Using the Taskflow API with Docker or Virtual Environments
----------------------------------------------------------

If you have tasks that require complex or conflicting requirements then you will have the ability to use the
Taskflow API with either a Docker container (since version 2.2.0) or Python virtual environment (since 2.0.2).
This added functionality will allow a much more
comprehensive range of use-cases for the Taskflow API, as you will not be limited to the
packages and system libraries of the Airflow worker.

To use a docker image with the Taskflow API, change the decorator to ``@task.docker``
and add any needed arguments to correctly run the task. Please note that the docker
image must have a working Python installed and take in a bash command as the ``command`` argument.

Below is an example of using the ``@task.docker`` decorator to run a python task.

.. exampleinclude:: /../../airflow/providers/docker/example_dags/tutorial_taskflow_api_etl_docker_virtualenv.py
    :language: python
    :dedent: 4
    :start-after: [START transform_docker]
    :end-before: [END transform_docker]

It is worth noting that the python source code (extracted from the decorated function) and any callable args are sent to the container via (encoded and pickled) environment variables so the length of these is not boundless (the exact limit depends on system settings).

.. note:: Using ``@task.docker`` decorator in one of the earlier Airflow versions

    Since ``@task.docker`` decorator is available in the docker provider, you might be tempted to use it in
    Airflow version before 2.2, but this is not going to work. You will get this error if you try:

    .. code-block:: text

        AttributeError: '_TaskDecorator' object has no attribute 'docker'

    You should upgrade to Airflow 2.2 or above in order to use it.

If you don't want to run your image on a Docker environment, and instead want to create a separate virtual
environment on the same machine, you can use the ``@task.virtualenv`` decorator instead. The ``@task.virtualenv``
decorator will allow you to create a new virtualenv with custom libraries and even a different
Python version to run your function.

.. exampleinclude:: /../../airflow/providers/docker/example_dags/tutorial_taskflow_api_etl_docker_virtualenv.py
    :language: python
    :dedent: 4
    :start-after: [START extract_virtualenv]
    :end-before: [END extract_virtualenv]

These two options should allow for far greater flexibility for users who wish to keep their workflows more simple
and pythonic.

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
