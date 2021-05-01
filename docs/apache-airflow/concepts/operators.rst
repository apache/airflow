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
        ping = SimpleHttpOperator(endpoint="http://example.com/update/")
        email = EmailOperator(to="admin@example.com", subject="Update complete")

        ping >> email

Airflow has a very extensive set of operators available, with some built-in to the core or pre-installed providers, like:

- :class:`~airflow.operators.bash.BashOperator` - executes a bash command
- :class:`~airflow.operators.python.PythonOperator` - calls an arbitrary Python function
- :class:`~airflow.operators.email.EmailOperator` - sends an email
- :class:`~airflow.providers.http.operators.http.SimpleHttpOperator` - sends an HTTP request
- :class:`~airflow.providers.sqlite.operators.sqlite.SqliteOperator` - SQLite DB operator

If the operator you need isn't installed with Airflow by default, you can probably find it as part of our huge set of community :doc:`apache-airflow-providers:index`. Some popular operators from here include:

- :class:`~airflow.providers.mysql.operators.mysql.MySqlOperator`
- :class:`~airflow.providers.postgres.operators.postgres.PostgresOperator`
- :class:`~airflow.providers.microsoft.mssql.operators.mssql.MsSqlOperator`
- :class:`~airflow.providers.oracle.operators.oracle.OracleOperator`
- :class:`~airflow.providers.jdbc.operators.jdbc.JdbcOperator`
- :class:`~airflow.providers.docker.operators.docker.DockerOperator`
- :class:`~airflow.providers.apache.hive.operators.hive.HiveOperator`
- :class:`~airflow.providers.amazon.aws.operators.s3_file_transform.S3FileTransformOperator`
- :class:`~airflow.providers.mysql.transfers.presto_to_mysql.PrestoToMySqlOperator`
- :class:`~airflow.providers.slack.operators.slack.SlackAPIOperator`

But there are many, many more - you can see the list of those in our :doc:`apache-airflow-providers:index` documentation.

.. note::

    Inside Airflow's code, we often mix the concepts of :doc:`tasks` and Operators, and they are mostly interchangeable. However, when we talk about a *Task*, we mean the generic "unit of execution" of a DAG; when we talk about an *Operator*, we mean a reusable, pre-made Task template whose logic is all done for you and that just needs some arguments.


.. _concepts:jinja-templating:

Jinja Templating
----------------
Airflow leverages the power of `Jinja Templating <http://jinja.pocoo.org/docs/dev/>`_ and this can be a powerful tool to use in combination with :doc:`macros </macros-ref>`.

For example, say you want to pass the execution date as an environment variable to a Bash script using the ``BashOperator``:

.. code-block:: python

  # The execution date as YYYY-MM-DD
  date = "{{ ds }}"
  t = BashOperator(
      task_id='test_env',
      bash_command='/tmp/test.sh ',
      dag=dag,
      env={'EXECUTION_DATE': date})

Here, ``{{ ds }}`` is a macro, and because the ``env`` parameter of the ``BashOperator`` is templated with Jinja, the execution date will be available as an environment variable named ``EXECUTION_DATE`` in your Bash script.

You can use Jinja templating with every parameter that is marked as "templated" in the documentation. Template substitution occurs just before the pre_execute function of your operator is called.

You can also use Jinja templating with nested fields, as long as these nested fields are marked as templated in the structure they belong to: fields registered in ``template_fields`` property will be submitted to template substitution, like the ``path`` field in the example below:

.. code-block:: python

    class MyDataReader:
        template_fields = ['path']

        def __init__(self, my_path):
            self.path = my_path

        # [additional code here...]

    t = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
        op_args=[
            MyDataReader('/tmp/{{ ds }}/my_file')
        ],
        dag=dag,
    )

.. note:: The ``template_fields`` property can equally be a class variable or an instance variable.

Deep nested fields can also be substituted, as long as all intermediate fields are marked as template fields:

.. code-block:: python

    class MyDataTransformer:
        template_fields = ['reader']

        def __init__(self, my_reader):
            self.reader = my_reader

        # [additional code here...]

    class MyDataReader:
        template_fields = ['path']

        def __init__(self, my_path):
            self.path = my_path

        # [additional code here...]

    t = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
        op_args=[
            MyDataTransformer(MyDataReader('/tmp/{{ ds }}/my_file'))
        ],
        dag=dag,
    )

You can pass custom options to the Jinja ``Environment`` when creating your DAG. One common usage is to avoid Jinja from dropping a trailing newline from a template string:

.. code-block:: python

    my_dag = DAG(
        dag_id='my-dag',
        jinja_environment_kwargs={
            'keep_trailing_newline': True,
             # some other jinja2 Environment options here
        },
    )

See the `Jinja documentation <https://jinja.palletsprojects.com/en/master/api/#jinja2.Environment>`_ to find all available options.
