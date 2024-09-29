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

.. _howto/operators:postgres:

How-to Guide for Postgres using SQLExecuteQueryOperator
=======================================================

Introduction
------------

Apache Airflow has a robust trove of operators that can be used to implement the various tasks that make up your
workflow. Airflow is essentially a graph (Directed Acyclic Graph) made up of tasks (nodes) and dependencies (edges).

A task defined or implemented by a operator is a unit of work in your data pipeline.

The purpose of this guide is to define tasks involving interactions with a PostgreSQL database with
the :class:`~airflow.providers.common.sql.operators.SQLExecuteQueryOperator`.

.. warning::
    Previously, PostgresOperator was used to perform this kind of operation. But at the moment PostgresOperator is deprecated and will be removed in future versions of the provider. Please consider to switch to SQLExecuteQueryOperator as soon as possible.

Common Database Operations with SQLExecuteQueryOperator
-------------------------------------------------------

To use the SQLExecuteQueryOperator to carry out PostgreSQL request, two parameters are required: ``sql`` and ``conn_id``.
These two parameters are eventually fed to the DbApiHook object that interacts directly with the Postgres database.

Creating a Postgres database table
----------------------------------

The code snippets below are based on Airflow-2.0

.. exampleinclude:: /../../tests/system/providers/postgres/example_postgres.py
    :language: python
    :start-after: [START postgres_sql_execute_query_operator_howto_guide]
    :end-before: [END postgres_sql_execute_query_operator_howto_guide_create_pet_table]


Dumping SQL statements into your operator isn't quite appealing and will create maintainability pains somewhere
down to the road. To prevent this, Airflow offers an elegant solution. This is how it works: you simply create
a directory inside the DAG folder called ``sql`` and then put all the SQL files containing your SQL queries inside it.

Your ``dags/sql/pet_schema.sql`` should like this:

::

      -- create pet table
      CREATE TABLE IF NOT EXISTS pet (
          pet_id SERIAL PRIMARY KEY,
          name VARCHAR NOT NULL,
          pet_type VARCHAR NOT NULL,
          birth_date DATE NOT NULL,
          OWNER VARCHAR NOT NULL);


Now let's refactor ``create_pet_table`` in our DAG:

.. code-block:: python

        create_pet_table = SQLExecuteQueryOperator(
            task_id="create_pet_table",
            conn_id="postgres_default",
            sql="sql/pet_schema.sql",
        )


Inserting data into a Postgres database table
---------------------------------------------

Let's say we already have the SQL insert statement below in our ``dags/sql/pet_schema.sql`` file:

::

  -- populate pet table
  INSERT INTO pet VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
  INSERT INTO pet VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
  INSERT INTO pet VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
  INSERT INTO pet VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');

We can then create a SQLExecuteQueryOperator task that populate the ``pet`` table.

.. code-block:: python

  populate_pet_table = SQLExecuteQueryOperator(
      task_id="populate_pet_table",
      conn_id="postgres_default",
      sql="sql/pet_schema.sql",
  )


Fetching records from your Postgres database table
--------------------------------------------------

Fetching records from your Postgres database table can be as simple as:

.. code-block:: python

  get_all_pets = SQLExecuteQueryOperator(
      task_id="get_all_pets",
      conn_id="postgres_default",
      sql="SELECT * FROM pet;",
  )



Passing Parameters into SQLExecuteQueryOperator for Postgres
------------------------------------------------------------

SQLExecuteQueryOperator provides ``parameters`` attribute which makes it possible to dynamically inject values into your
SQL requests during runtime. The BaseOperator class has the ``params`` attribute which is available to the SQLExecuteQueryOperator
by virtue of inheritance. While both ``parameters`` and ``params`` make it possible to dynamically pass in parameters in many
interesting ways, their usage is slightly different as demonstrated in the examples below.

To find the birthdates of all pets between two dates, when we use the SQL statements directly in our code, we will use the 
``parameters`` attribute:

.. code-block:: python

  get_birth_date = SQLExecuteQueryOperator(
      task_id="get_birth_date",
      conn_id="postgres_default",
      sql="SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
      parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
  )

Now lets refactor our ``get_birth_date`` task. Now, instead of dumping SQL statements directly into our code, let's tidy things up
by creating a sql file. And this time we will use the ``params`` attribute which we get for free from the parent ``BaseOperator``
class.

::

  -- dags/sql/birth_date.sql
  SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC {{ params.begin_date }} AND {{ params.end_date }};


.. code-block:: python

  get_birth_date = SQLExecuteQueryOperator(
      task_id="get_birth_date",
      conn_id="postgres_default",
      sql="sql/birth_date.sql",
      params={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
  )


Enable logging of database messages sent to the client
-------------------------------------------------------------

SQLExecuteQueryOperator provides ``hook_params`` attribute that allows you to pass add parameters to DbApiHook.
You can use ``enable_log_db_messages`` to log database messages or errors emitted by the ``RAISE`` statement.

.. code-block:: python

  call_proc = SQLExecuteQueryOperator(
      task_id="call_proc",
      conn_id="postgres_default",
      sql="call proc();",
      hook_params={"enable_log_db_messages": True},
  )


Passing Server Configuration Parameters into PostgresOperator
-------------------------------------------------------------

SQLExecuteQueryOperator provides ``hook_params`` attribute that allows you to pass add parameters to DbApiHook.
You can pass ``options`` argument this way so that you specify `command-line options <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-OPTIONS>`_
sent to the server at connection start.

.. exampleinclude:: /../../tests/system/providers/postgres/example_postgres.py
    :language: python
    :start-after: [START postgres_sql_execute_query_operator_howto_guide_get_birth_date]
    :end-before: [END postgres_sql_execute_query_operator_howto_guide_get_birth_date]


The complete Postgres Operator DAG
----------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../tests/system/providers/postgres/example_postgres.py
    :language: python
    :start-after: [START postgres_sql_execute_query_operator_howto_guide]
    :end-before: [END postgres_sql_execute_query_operator_howto_guide]


Conclusion
----------

In this how-to guide we explored the Apache Airflow SQLExecuteQueryOperator to connect to PostgreSQL Database. Let's quickly highlight the key takeaways.
It is best practice to create subdirectory called ``sql`` in your ``dags`` directory where you can store your sql files.
This will make your code more elegant and more maintainable.
And finally, we looked at the different ways you can dynamically pass parameters into our PostgresOperator
tasks using ``parameters`` or ``params`` attribute and how you can control the session parameters by passing
options in the ``hook_params`` attribute.
