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




Working with TaskFlow
=====================

This tutorial builds on the regular Airflow Tutorial and focuses specifically
on writing data pipelines using the TaskFlow API paradigm which is introduced as
part of Airflow 2.0 and contrasts this with DAGs written using the traditional paradigm.

The data pipeline chosen here is a simple pattern with
three separate Extract, Transform, and Load tasks.

Example "TaskFlow API" Pipeline
-------------------------------

Here is a very simple pipeline using the TaskFlow API paradigm. A more detailed
explanation is given below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api.py
    :language: python
    :start-after: [START tutorial]
    :end-before: [END tutorial]

It's a DAG definition file
--------------------------

If this is the first DAG file you are looking at, please note that this Python script
is interpreted by Airflow and is a configuration file for your data pipeline.
For a complete introduction to DAG files, please look at the core :doc:`fundamentals tutorial<fundamentals>`
which covers DAG structure and definitions extensively.


Instantiate a DAG
-----------------

We are creating a DAG which is the collection of our tasks with dependencies between
the tasks. This is a very simple definition, since we just want the DAG to be run
when we set this up with Airflow, without any retries or complex scheduling.
In this example, please notice that we are creating this DAG using the ``@dag`` decorator
as shown below, with the Python function name acting as the DAG identifier.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api.py
    :language: python
    :start-after: [START instantiate_dag]
    :end-before: [END instantiate_dag]

Now to actually enable this to be run as a DAG, we invoke the Python function
``tutorial_taskflow_api`` set up using the ``@dag`` decorator earlier, as shown below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api.py
    :language: python
    :start-after: [START dag_invocation]
    :end-before: [END dag_invocation]

.. versionchanged:: 2.4

      It's no longer required to "register" the DAG into a global variable for Airflow to be able to detect the dag if that DAG is used inside a ``with`` block, or if it is the result of a ``@dag`` decorated function.

Tasks
-----
In this data pipeline, tasks are created based on Python functions using the ``@task`` decorator
as shown below. The function name acts as a unique identifier for the task.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api.py
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

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api.py
    :language: python
    :dedent: 4
    :start-after: [START main_flow]
    :end-before: [END main_flow]

That's it, we are done!
We have invoked the Extract task, obtained the order data from there and sent it over to
the Transform task for summarization, and then invoked the Load task with the summarized data.
The dependencies between the tasks and the passing of data between these tasks which could be
running on different workers on different nodes on the network is all handled by Airflow.

Now to actually enable this to be run as a DAG, we invoke the Python function
``tutorial_taskflow_api`` set up using the ``@dag`` decorator earlier, as shown below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api.py
    :language: python
    :start-after: [START dag_invocation]
    :end-before: [END dag_invocation]


But how?
--------
For experienced Airflow DAG authors, this is startlingly simple! Let's contrast this with
how this DAG had to be written before Airflow 2.0 below:

.. exampleinclude:: /../../airflow/example_dags/tutorial_dag.py
    :language: python
    :start-after: [START tutorial]
    :end-before: [END tutorial]

All of the processing shown above is being done in the new Airflow 2.0 DAG as well, but
it is all abstracted from the DAG developer.

Let's examine this in detail by looking at the Transform task in isolation since it is
in the middle of the data pipeline. In Airflow 1.x, this task is defined as shown below:

.. exampleinclude:: /../../airflow/example_dags/tutorial_dag.py
    :language: python
    :dedent: 4
    :start-after: [START transform_function]
    :end-before: [END transform_function]

As we see here, the data being processed in the Transform function is passed to it using XCom
variables. In turn, the summarized data from the Transform function is also placed
into another XCom variable which will then be used by the Load task.

Contrasting that with TaskFlow API in Airflow 2.0 as shown below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api.py
    :language: python
    :dedent: 4
    :start-after: [START transform]
    :end-before: [END transform]

All of the XCom usage for data passing between these tasks is abstracted away from the DAG author
in Airflow 2.0. However, XCom variables are used behind the scenes and can be viewed using
the Airflow UI as necessary for debugging or DAG monitoring.

Similarly, task dependencies are automatically generated within TaskFlows based on the
functional invocation of tasks. In Airflow 1.x, tasks had to be explicitly created and
dependencies specified as shown below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_dag.py
    :language: python
    :dedent: 4
    :start-after: [START main_flow]
    :end-before: [END main_flow]

In contrast, with the TaskFlow API in Airflow 2.0, the invocation itself automatically generates
the dependencies as shown below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api.py
    :language: python
    :dedent: 4
    :start-after: [START main_flow]
    :end-before: [END main_flow]


Reusing a decorated task
-------------------------

Decorated tasks are flexible. You can reuse a decorated task in multiple DAGs, overriding the task
parameters such as the ``task_id``, ``queue``, ``pool``, etc.

Below is an example of how you can reuse a decorated task in multiple DAGs:

.. code-block:: python

    from airflow.decorators import task, dag
    from datetime import datetime


    @task
    def add_task(x, y):
        print(f"Task args: x={x}, y={y}")
        return x + y


    @dag(start_date=datetime(2022, 1, 1))
    def mydag():
        start = add_task.override(task_id="start")(1, 2)
        for i in range(3):
            start >> add_task.override(task_id=f"add_start_{i}")(start, i)


    @dag(start_date=datetime(2022, 1, 1))
    def mydag2():
        start = add_task(1, 2)
        for i in range(3):
            start >> add_task.override(task_id=f"new_add_task_{i}")(start, i)


    first_dag = mydag()
    second_dag = mydag2()

You can also import the above ``add_task`` and use it in another DAG file.
Suppose the ``add_task`` code lives in a file called ``common.py``. You can do this:

.. code-block:: python

    from common import add_task
    from airflow.decorators import dag
    from datetime import datetime


    @dag(start_date=datetime(2022, 1, 1))
    def use_add_task():
        start = add_task.override(priority_weight=3)(1, 2)
        for i in range(3):
            start >> add_task.override(task_id=f"new_add_task_{i}", retries=4)(start, i)


    created_dag = use_add_task()


Using the TaskFlow API with complex/conflicting Python dependencies
-------------------------------------------------------------------

If you have tasks that require complex or conflicting requirements then you will have the ability to use the
TaskFlow API with either Python virtual environment (since 2.0.2), Docker container (since 2.2.0), ExternalPythonOperator (since 2.4.0) or KubernetesPodOperator (since 2.4.0).

This functionality allows a much more comprehensive range of use-cases for the TaskFlow API,
as you are not limited to the packages and system libraries of the Airflow worker. For all cases of
the decorated functions described below, you have to make sure the functions are serializable and that
they only use local imports for additional dependencies you use. Those imported additional libraries must
be available in the target environment - they do not need to be available in the main Airflow environment.

Which of the operators you should use, depend on several factors:

* whether you are running Airflow with access to Docker engine or Kubernetes
* whether you can afford an overhead to dynamically create a virtual environment with the new dependencies
* whether you can deploy a pre-existing, immutable Python environment for all Airflow components.

These options should allow for far greater flexibility for users who wish to keep their workflows simpler
and more Pythonic - and allow you to keep complete logic of your DAG in the DAG itself.

You can also get more context about the approach of managing conflicting dependencies, including more detailed
explanation on boundaries and consequences of each of the options in
:ref:`Best practices for handling conflicting/complex Python dependencies <best_practices/handling_conflicting_complex_python_dependencies>`


Virtualenv created dynamically for each task
............................................

The simplest approach is to create dynamically (every time a task is run) a separate virtual environment on the
same machine, you can use the ``@task.virtualenv`` decorator. The decorator allows
you to create dynamically a new virtualenv with custom libraries and even a different Python version to
run your function.

.. _taskflow/virtualenv_example:

Example (dynamically created virtualenv):

.. exampleinclude:: /../../airflow/example_dags/example_python_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_python_venv]
    :end-before: [END howto_operator_python_venv]

Using Python environment with pre-installed dependencies
........................................................

A bit more involved ``@task.external_python`` decorator allows you to run an Airflow task in pre-defined,
immutable virtualenv (or Python binary installed at system level without virtualenv).
This virtualenv or system python can also have different set of custom libraries installed and must be
made available in all workers that can execute the tasks in the same location.

.. _taskflow/external_python_example:

Example with ``@task.external_python`` (using immutable, pre-existing virtualenv):

.. exampleinclude:: /../../airflow/example_dags/example_python_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_external_python]
    :end-before: [END howto_operator_external_python]

Dependency separation using Docker Operator
...........................................

If your Airflow workers have access to a docker engine, you can instead use a ``DockerOperator``
and add any needed arguments to correctly run the task. Please note that the docker
image must have a working Python installed and take in a bash command as the ``command`` argument.

It is worth noting that the Python source code (extracted from the decorated function) and any
callable args are sent to the container via (encoded and pickled) environment variables so the
length of these is not boundless (the exact limit depends on system settings).

Below is an example of using the ``@task.docker`` decorator to run a Python task.

.. _taskflow/docker_example:

.. exampleinclude:: /../../providers/tests/system/docker/example_taskflow_api_docker_virtualenv.py
    :language: python
    :dedent: 4
    :start-after: [START transform_docker]
    :end-before: [END transform_docker]


Notes on using the operator:

.. note:: Using ``@task.docker`` decorator in one of the earlier Airflow versions

    Since ``@task.docker`` decorator is available in the docker provider, you might be tempted to use it in
    Airflow version before 2.2, but this is not going to work. You will get this error if you try:

    .. code-block:: text

        AttributeError: '_TaskDecorator' object has no attribute 'docker'

    You should upgrade to Airflow 2.2 or above in order to use it.

Dependency separation using Kubernetes Pod Operator
...................................................


If your Airflow workers have access to Kubernetes, you can instead use a ``KubernetesPodOperator``
and add any needed arguments to correctly run the task.

Below is an example of using the ``@task.kubernetes`` decorator to run a Python task.

.. _taskflow/kubernetes_example:

.. exampleinclude:: /../../providers/tests/system/cncf/kubernetes/example_kubernetes_decorator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_kubernetes]
    :end-before: [END howto_operator_kubernetes]

Notes on using the operator:

.. note:: Using ``@task.kubernetes`` decorator in one of the earlier Airflow versions

    Since ``@task.kubernetes`` decorator is available in the docker provider, you might be tempted to use it in
    Airflow version before 2.4, but this is not going to work. You will get this error if you try:

    .. code-block:: text

        AttributeError: '_TaskDecorator' object has no attribute 'kubernetes'

    You should upgrade to Airflow 2.4 or above in order to use it.


Using the TaskFlow API with Sensor operators
--------------------------------------------

You can apply the ``@task.sensor`` decorator to convert a regular Python function to an instance of the
BaseSensorOperator class. The Python function implements the poke logic and returns an instance of
the ``PokeReturnValue`` class as the ``poke()`` method in the BaseSensorOperator does.
In Airflow 2.3, sensor operators will be able to return XCOM values. This is achieved by returning
an instance of the ``PokeReturnValue`` object at the end of the ``poke()`` method:

  .. code-block:: python

    from airflow.sensors.base import PokeReturnValue


    class SensorWithXcomValue(BaseSensorOperator):
        def poke(self, context: Context) -> Union[bool, PokeReturnValue]:
            # ...
            is_done = ...  # set to true if the sensor should stop poking.
            xcom_value = ...  # return value of the sensor operator to be pushed to XCOM.
            return PokeReturnValue(is_done, xcom_value)


To implement a sensor operator that pushes a XCOM value and supports both version 2.3 and
pre-2.3, you need to explicitly push the XCOM value if the version is pre-2.3.

  .. code-block:: python

    try:
        from airflow.sensors.base import PokeReturnValue
    except ImportError:
        PokeReturnValue = None


    class SensorWithXcomValue(BaseSensorOperator):
        def poke(self, context: Context) -> bool:
            # ...
            is_done = ...  # set to true if the sensor should stop poking.
            xcom_value = ...  # return value of the sensor operator to be pushed to XCOM.
            if PokeReturnValue is not None:
                return PokeReturnValue(is_done, xcom_value)
            else:
                if is_done:
                    context["ti"].xcom_push(key="xcom_key", value=xcom_value)
                return is_done




Alternatively in cases where the sensor doesn't need to push XCOM values:  both ``poke()`` and the wrapped
function can return a boolean-like value where ``True`` designates the sensor's operation as complete and
``False`` designates the sensor's operation as incomplete.

.. _taskflow/task_sensor_example:

.. exampleinclude:: /../../airflow/example_dags/example_sensor_decorator.py
    :language: python
    :start-after: [START tutorial]
    :end-before: [END tutorial]


Multiple outputs inference
--------------------------
Tasks can also infer multiple outputs by using dict Python typing.

.. code-block:: python

   @task
   def identity_dict(x: int, y: int) -> dict[str, int]:
       return {"x": x, "y": y}

By using the typing ``dict``, or any other class that conforms to the ``typing.Mapping`` protocol,
for the function return type, the ``multiple_outputs`` parameter is automatically set to true.

Note, If you manually set the ``multiple_outputs`` parameter the inference is disabled and
the parameter value is used.

Adding dependencies between decorated and traditional tasks
-----------------------------------------------------------
The above tutorial shows how to create dependencies between TaskFlow functions. However, dependencies can also
be set between traditional tasks (such as :class:`~airflow.providers.standard.operators.bash.BashOperator`
or :class:`~airflow.sensors.filesystem.FileSensor`) and TaskFlow functions.

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
        return order_data_df


    file_task = FileSensor(task_id="check_file", filepath="/tmp/order_data.csv")
    order_data = extract_from_file()

    file_task >> order_data


In the above code block, a new TaskFlow function is defined as ``extract_from_file`` which
reads the data from a known file location.
In the main DAG, a new ``FileSensor`` task is defined to check for this file. Please note
that this is a Sensor task which waits for the file.
The TaskFlow function call is put in a variable ``order_data``.
Finally, a dependency between this Sensor task and the TaskFlow function is specified using the variable.


Consuming XComs between decorated and traditional tasks
-------------------------------------------------------
As noted above, the TaskFlow API allows XComs to be consumed or passed between tasks in a manner that is
abstracted away from the DAG author. This section dives further into detailed examples of how this is
possible not only between TaskFlow functions but between both TaskFlow functions *and* traditional tasks.

You may find it necessary to consume an XCom from traditional tasks, either pushed within the task's execution
or via its return value, as an input into downstream tasks. You can access the pushed XCom (also known as an
``XComArg``) by utilizing the ``.output`` property exposed for all operators.

By default, using the ``.output`` property to retrieve an XCom result is the equivalent of:

.. code-block:: python

    task_instance.xcom_pull(task_ids="my_task_id", key="return_value")

To retrieve an XCom result for a key other than ``return_value``, you can use:

.. code-block:: python

    my_op = MyOperator(...)
    my_op_output = my_op.output["some_other_xcom_key"]
    # OR
    my_op_output = my_op.output.get("some_other_xcom_key")

.. note::
    Using the ``.output`` property as an input to another task is supported only for operator parameters
    listed as a ``template_field``.

In the code example below, a :class:`~airflow.providers.http.operators.http.HttpOperator` result
is captured via :doc:`XComs </core-concepts/xcoms>`. This XCom result, which is the task output, is then passed
to a TaskFlow function which parses the response as JSON.

.. code-block:: python

    get_api_results_task = HttpOperator(
        task_id="get_api_results",
        endpoint="/api/query",
        do_xcom_push=True,
        http_conn_id="http",
    )


    @task
    def parse_results(api_results):
        return json.loads(api_results)


    parsed_results = parse_results(api_results=get_api_results_task.output)

The reverse can also be done: passing the output of a TaskFlow function as an input to a traditional task.

.. code-block:: python

    @task(retries=3)
    def create_queue():
        """This is a Python function that creates an SQS queue"""
        hook = SqsHook()
        result = hook.create_queue(queue_name="sample-queue")

        return result["QueueUrl"]


    sqs_queue = create_queue()

    publish_to_queue = SqsPublishOperator(
        task_id="publish_to_queue",
        sqs_queue=sqs_queue,
        message_content="{{ task_instance }}-{{ execution_date }}",
        message_attributes=None,
        delay_seconds=0,
    )

Take note in the code example above, the output from the ``create_queue`` TaskFlow function, the URL of a
newly-created Amazon SQS Queue, is then passed to a :class:`~airflow.providers.amazon.aws.operators.sqs.SqsPublishOperator`
task as the ``sqs_queue`` arg.

Finally, not only can you use traditional operator outputs as inputs for TaskFlow functions, but also as inputs to
other traditional operators. In the example below, the output from the :class:`~airflow.providers.amazon.aws.transfers.salesforce_to_s3.SalesforceToS3Operator`
task (which is an S3 URI for a destination file location) is used an input for the :class:`~airflow.providers.amazon.aws.operators.s3_copy_object.S3CopyObjectOperator`
task to copy the same file to a date-partitioned storage location in S3 for long-term storage in a data lake.

.. code-block:: python

    BASE_PATH = "salesforce/customers"
    FILE_NAME = "customer_daily_extract_{{ ds_nodash }}.csv"


    upload_salesforce_data_to_s3_landing = SalesforceToS3Operator(
        task_id="upload_salesforce_data_to_s3",
        salesforce_query="SELECT Id, Name, Company, Phone, Email, LastModifiedDate, IsActive FROM Customers",
        s3_bucket_name="landing-bucket",
        s3_key=f"{BASE_PATH}/{FILE_NAME}",
        salesforce_conn_id="salesforce",
        aws_conn_id="s3",
        replace=True,
    )


    store_to_s3_data_lake = S3CopyObjectOperator(
        task_id="store_to_s3_data_lake",
        aws_conn_id="s3",
        source_bucket_key=upload_salesforce_data_to_s3_landing.output,
        dest_bucket_name="data_lake",
        dest_bucket_key=f"""{BASE_PATH}/{"{{ execution_date.strftime('%Y/%m/%d') }}"}/{FILE_NAME}""",
    )

.. _taskflow/accessing_context_variables:

Accessing context variables in decorated tasks
----------------------------------------------

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

.. versionchanged:: 2.8
    Previously the context key arguments must provide a default, e.g. ``ti=None``.
    This is no longer needed.

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

    from airflow.providers.standard.operators.python import get_current_context


    def some_function_in_your_library():
        context = get_current_context()
        ti = context["ti"]

Current context is accessible only during the task execution. The context is not accessible during
``pre_execute`` or ``post_execute``. Calling this method outside execution context will raise an error.

Using templates in decorated tasks
----------------------------------------------

Arguments passed to your decorated function are automatically templated.

You can also use the ``templates_exts`` parameter to template entire files.

.. code-block:: python

    @task(templates_exts=[".sql"])
    def template_test(sql):
        print(f"sql: {sql}")


    template_test(sql="sql/test.sql")

This will read the content of ``sql/test.sql`` and replace all template variables. You can also pass a list of files and all of them will be templated.

You can pass additional parameters to the template engine through `the params parameter </concepts/params.html>`_.

However, the ``params`` parameter must be passed to the decorator and not to your function directly, such as ``@task(templates_exts=['.sql'], params={'my_param'})`` and can then be used with ``{{ params.my_param }}`` in your templated files and function parameters.

Alternatively, you can also pass it using the ``.override()`` method:

.. code-block:: python

    @task()
    def template_test(input_var):
        print(f"input_var: {input_var}")


    template_test.override(params={"my_param": "wow"})(
        input_var="my param is: {{ params.my_param }}",
    )

Finally, you can also manually render templates:

.. code-block:: python

    @task(params={"my_param": "wow"})
    def template_test():
        template_str = "run_id: {{ run_id }}; params.my_param: {{ params.my_param }}"

        context = get_current_context()
        rendered_template = context["task"].render_template(
            template_str,
            context,
        )

Here is a full example that demonstrates everything above:

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_templates.py
    :language: python
    :start-after: [START tutorial]
    :end-before: [END tutorial]

Conditionally skipping tasks
----------------------------

The ``run_if()`` and ``skip_if()`` are syntactic sugar for TaskFlow
that allows you to skip a ``Task`` based on a condition.
You can use them to simply set execution conditions
without changing the structure of the ``DAG`` or ``Task``.

It also allows you to set conditions using ``Context``,
which is essentially the same as using ``pre_execute``.

An example usage of ``run_if()`` is as follows:

.. code-block:: python

    @task.run_if(lambda context: context["task_instance"].task_id == "run")
    @task.bash()
    def echo() -> str:
        return "echo 'run'"

The ``echo`` defined in the above code is only executed when the ``task_id`` is ``run``.

If you want to leave a log when you skip a task, you have two options.

.. tab-set::

    .. tab-item:: Static message

        .. code-block:: python

            @task.run_if(lambda context: context["task_instance"].task_id == "run", skip_message="only task_id is 'run'")
            @task.bash()
            def echo() -> str:
                return "echo 'run'"

    .. tab-item:: using Context

        .. code-block:: python

            @task.run_if(
                lambda context: (context["task_instance"].task_id == "run", f"{context['ts']}: only task_id is 'run'")
            )
            @task.bash()
            def echo() -> str:
                return "echo 'run'"

There is also a ``skip_if()`` that works the opposite of ``run_if()``, and is used in the same way.

.. code-block:: python

    @task.skip_if(lambda context: context["task_instance"].task_id == "skip")
    @task.bash()
    def echo() -> str:
        return "echo 'run'"

What's Next?
------------

You have seen how simple it is to write DAGs using the TaskFlow API paradigm within Airflow 2.0. Here are a few steps you might want to take next:

.. seealso::
    - Continue to the next step of the tutorial: :doc:`/tutorial/pipeline`
    - Read the :doc:`Concepts section </core-concepts/index>` for detailed explanation of Airflow concepts such as DAGs, Tasks, Operators, and more
    - View the section on the :doc:`TaskFlow API </core-concepts/taskflow>` and the ``@task`` decorator.
