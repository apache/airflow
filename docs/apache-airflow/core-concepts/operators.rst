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

Operators
=========

An Operator is conceptually a template for a predefined :doc:`Task <tasks>`, that you can just define declaratively inside your DAG::

    with DAG("my-dag") as dag:
        ping = HttpOperator(endpoint="http://example.com/update/")
        email = EmailOperator(to="admin@example.com", subject="Update complete")

        ping >> email

Airflow has a very extensive set of operators available, with some built-in to the core or pre-installed providers. Some popular operators from core include:

- :class:`~airflow.providers.standard.operators.bash.BashOperator` - executes a bash command
- :class:`~airflow.operators.python.PythonOperator` - calls an arbitrary Python function
- :class:`~airflow.operators.email.EmailOperator` - sends an email
- Use the ``@task`` decorator to execute an arbitrary Python function. It doesn't support rendering jinja templates passed as arguments.

.. note::
    The ``@task`` decorator is recommended over the classic :class:`~airflow.operators.python.PythonOperator`
    to execute Python callables with no template rendering in its arguments.

For a list of all core operators, see: :doc:`Core Operators and Hooks Reference </operators-and-hooks-ref>`.

If the operator you need isn't installed with Airflow by default, you can probably find it as part of our huge set of community :doc:`provider packages <apache-airflow-providers:index>`. Some popular operators from here include:

- :class:`~airflow.providers.http.operators.http.HttpOperator`
- :class:`~airflow.providers.mysql.operators.mysql.MySqlOperator`
- :class:`~airflow.providers.postgres.operators.postgres.PostgresOperator`
- :class:`~airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`
- :class:`~airflow.providers.microsoft.mssql.operators.mssql.MsSqlOperator`
- :class:`~airflow.providers.oracle.operators.oracle.OracleOperator`
- :class:`~airflow.providers.jdbc.operators.jdbc.JdbcOperator`
- :class:`~airflow.providers.docker.operators.docker.DockerOperator`
- :class:`~airflow.providers.apache.hive.operators.hive.HiveOperator`
- :class:`~airflow.providers.amazon.aws.operators.s3.S3FileTransformOperator`
- :class:`~airflow.providers.mysql.transfers.presto_to_mysql.PrestoToMySqlOperator`
- :class:`~airflow.providers.slack.operators.slack.SlackAPIOperator`

But there are many, many more - you can see the full list of all community-managed operators, hooks, sensors
and transfers in our
:doc:`providers packages <apache-airflow-providers:operators-and-hooks-ref/index>` documentation.

.. note::

    Inside Airflow's code, we often mix the concepts of :doc:`tasks` and Operators, and they are mostly
    interchangeable. However, when we talk about a *Task*, we mean the generic "unit of execution" of a
    DAG; when we talk about an *Operator*, we mean a reusable, pre-made Task template whose logic
    is all done for you and that just needs some arguments.


.. _concepts:jinja-templating:

Jinja Templating
----------------
Airflow leverages the power of `Jinja Templating <http://jinja.pocoo.org/docs/dev/>`_ and this can be a powerful tool to use in combination with :ref:`macros <templates-ref>`.

For example, say you want to pass the start of the data interval as an environment variable to a Bash script using the ``BashOperator``:

.. code-block:: python

  # The start of the data interval as YYYY-MM-DD
  date = "{{ ds }}"
  t = BashOperator(
      task_id="test_env",
      bash_command="/tmp/test.sh ",
      dag=dag,
      env={"DATA_INTERVAL_START": date},
  )

Here, ``{{ ds }}`` is a templated variable, and because the ``env`` parameter of the ``BashOperator`` is templated with Jinja, the data interval's start date will be available as an environment variable named ``DATA_INTERVAL_START`` in your Bash script.

You can also pass in a callable instead when Python is more readable than a Jinja template. The callable must accept two named arguments ``context`` and ``jinja_env``:

.. code-block:: python

    def build_complex_command(context, jinja_env):
        with open("file.csv") as f:
            return do_complex_things(f)


    t = BashOperator(
        task_id="complex_templated_echo",
        bash_command=build_complex_command,
        dag=dag,
    )

Since each template field is only rendered once, the callable's return value will not go through rendering again. Therefore, the callable must manually render any templates. This can be done by calling ``render_template()`` on the current task like this:

.. code-block:: python

    def build_complex_command(context, jinja_env):
        with open("file.csv") as f:
            data = do_complex_things(f)
        return context["task"].render_template(data, context, jinja_env)

You can use templating with every parameter that is marked as "templated" in the documentation. Template substitution occurs just before the ``pre_execute`` function of your operator is called.

You can also use templating with nested fields, as long as these nested fields are marked as templated in the structure they belong to: fields registered in ``template_fields`` property will be submitted to template substitution, like the ``path`` field in the example below:

.. code-block:: python

    class MyDataReader:
        template_fields: Sequence[str] = ("path",)

        def __init__(self, my_path):
            self.path = my_path

        # [additional code here...]


    t = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_args=[MyDataReader("/tmp/{{ ds }}/my_file")],
        dag=dag,
    )


.. note:: The ``template_fields`` property is a class variable and guaranteed to be of a ``Sequence[str]``
    type (i.e. a list or tuple of strings).

Deep nested fields can also be substituted, as long as all intermediate fields are marked as template fields:

.. code-block:: python

    class MyDataTransformer:
        template_fields: Sequence[str] = ("reader",)

        def __init__(self, my_reader):
            self.reader = my_reader

        # [additional code here...]


    class MyDataReader:
        template_fields: Sequence[str] = ("path",)

        def __init__(self, my_path):
            self.path = my_path

        # [additional code here...]


    t = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_args=[MyDataTransformer(MyDataReader("/tmp/{{ ds }}/my_file"))],
        dag=dag,
    )


You can pass custom options to the Jinja ``Environment`` when creating your DAG. One common usage is to avoid Jinja from dropping a trailing newline from a template string:

.. code-block:: python

    my_dag = DAG(
        dag_id="my-dag",
        jinja_environment_kwargs={
            "keep_trailing_newline": True,
            # some other jinja2 Environment options here
        },
    )

See the `Jinja documentation <https://jinja.palletsprojects.com/en/2.11.x/api/#jinja2.Environment>`_ to find all available options.

Some operators will also consider strings ending in specific suffixes (defined in ``template_ext``) to be references to files when rendering fields. This can be useful for loading scripts or queries directly from files rather than including them into DAG code.

For example, consider a BashOperator which runs a multi-line bash script, this will load the file at ``script.sh`` and use its contents as the value for ``bash_command``:

.. code-block:: python

    run_script = BashOperator(
        task_id="run_script",
        bash_command="script.sh",
    )

By default, paths provided in this way should be provided relative to the DAG's folder (as this is the default Jinja template search path), but additional paths can be added by setting the ``template_searchpath`` arg on the DAG.

In some cases, you may want to exclude a string from templating and use it directly. Consider the following task:

.. code-block:: python

    print_script = BashOperator(
        task_id="print_script",
        bash_command="cat script.sh",
    )

This will fail with ``TemplateNotFound: cat script.sh`` since Airflow would treat the string as a path to a file, not a command.
We can prevent airflow from treating this value as a reference to a file by wrapping it in :func:`~airflow.util.template.literal`.
This approach disables the rendering of both macros and files and can be applied to selected nested fields while retaining the default templating rules for the remainder of the content.

.. code-block:: python

    from airflow.utils.template import literal


    fixed_print_script = BashOperator(
        task_id="fixed_print_script",
        bash_command=literal("cat script.sh"),
    )

.. versionadded:: 2.8
    :func:`~airflow.util.template.literal` was added.

Alternatively, if you want to prevent Airflow from treating a value as a reference to a file, you can override ``template_ext``:

.. code-block:: python

    fixed_print_script = BashOperator(
        task_id="fixed_print_script",
        bash_command="cat script.sh",
    )
    fixed_print_script.template_ext = ()


.. _concepts:templating-native-objects:

Rendering Fields as Native Python Objects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, all Jinja templates in ``template_fields`` are rendered as strings. This however is not always desired. For example, let's say an ``extract`` task pushes a dictionary ``{"1001": 301.27, "1002": 433.21, "1003": 502.22}`` to :ref:`XCom <concepts:xcom>`:

.. code-block:: python

    @task(task_id="extract")
    def extract():
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        return json.loads(data_string)


If a task depends on ``extract``, ``order_data`` argument is passed a string ``"{'1001': 301.27, '1002': 433.21, '1003': 502.22}"``:

.. code-block:: python

    def transform(order_data):
        total_order_value = sum(order_data.values())  # Fails because order_data is a str :(
        return {"total_order_value": total_order_value}


    transform = PythonOperator(
        task_id="transform",
        op_kwargs={"order_data": "{{ ti.xcom_pull('extract') }}"},
        python_callable=transform,
    )

    extract() >> transform

There are two solutions if we want to get the actual dict instead. The first is to use a callable:

.. code-block:: python

    def render_transform_op_kwargs(context, jinja_env):
        order_data = context["ti"].xcom_pull("extract")
        return {"order_data": order_data}


    transform = PythonOperator(
        task_id="transform",
        op_kwargs=render_transform_op_kwargs,
        python_callable=transform,
    )

Alternatively, Jinja can also be instructed to render a native Python object. This is done by passing ``render_template_as_native_obj=True`` to the DAG. This makes Airflow use `NativeEnvironment <https://jinja.palletsprojects.com/en/2.11.x/nativetypes/>`_ instead of the default ``SandboxedEnvironment``:

.. code-block:: python

    with DAG(
        dag_id="example_template_as_python_object",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        render_template_as_native_obj=True,
    ):
        transform = PythonOperator(
            task_id="transform",
            op_kwargs={"order_data": "{{ ti.xcom_pull('extract') }}"},
            python_callable=transform,
        )


.. _concepts:reserved-keywords:

Reserved params keyword
-----------------------

In Apache Airflow 2.2.0 ``params`` variable is used during DAG serialization. Please do not use that name in third party operators.
If you upgrade your environment and get the following error:

.. code-block::

    AttributeError: 'str' object has no attribute '__module__'

change name from ``params`` in your operators.
