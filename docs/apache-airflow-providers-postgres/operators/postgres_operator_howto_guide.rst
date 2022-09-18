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

How-to Guide for PostgresOperator
=================================

Introduction
------------

Apache Airflow has a robust trove of operators that can be used to implement the various tasks that make up your
workflow. Airflow is essentially a graph (Directed Acyclic Graph) made up of tasks (nodes) and dependencies (edges).

A task defined or implemented by a operator is a unit of work in your data pipeline.

The purpose of Postgres Operator is to define tasks involving interactions with a PostgreSQL database.
 In ``Airflow-2.0``, the ``PostgresOperator`` class resides at ``airflow.providers.postgres.operators.postgres``.

Under the hood, the :class:`~airflow.providers.postgres.operators.postgres.PostgresOperator` delegates its heavy lifting to the :class:`~airflow.providers.postgres.hooks.postgres.PostgresHook`.

Common Database Operations with PostgresOperator
------------------------------------------------

To use the PostgresOperator to carry out SQL request, two parameters are required: ``sql`` and ``postgres_conn_id``.
These two parameters are eventually fed to the PostgresHook object that interacts directly with the Postgres database.

Creating a Postgres database table
----------------------------------

The code snippets below are based on Airflow-2.0

.. exampleinclude:: /../../tests/system/providers/postgres/example_postgres.py
    :language: python
    :start-after: [START postgres_operator_howto_guide]
    :end-before: [END postgres_operator_howto_guide_create_pet_table]


Dumping SQL statements into your PostgresOperator isn't quite appealing and will create maintainability pains somewhere
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

        create_pet_table = PostgresOperator(
            task_id="create_pet_table",
            postgres_conn_id="postgres_default",
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

We can then create a PostgresOperator task that populate the ``pet`` table.

.. code-block:: python

  populate_pet_table = PostgresOperator(
      task_id="populate_pet_table",
      postgres_conn_id="postgres_default",
      sql="sql/pet_schema.sql",
  )


Fetching records from your Postgres database table
--------------------------------------------------

Fetching records from your Postgres database table can be as simple as:

.. code-block:: python

  get_all_pets = PostgresOperator(
      task_id="get_all_pets",
      postgres_conn_id="postgres_default",
      sql="SELECT * FROM pet;",
  )



Passing Parameters into PostgresOperator
----------------------------------------

PostgresOperator provides ``parameters`` attribute which makes it possible to dynamically inject values into your
SQL requests during runtime. The BaseOperator class has the ``params`` attribute which is available to the PostgresOperator
by virtue of inheritance. Both ``parameters`` and ``params`` make it possible to dynamically pass in parameters in many
interesting ways.

To find the owner of the pet called 'Lester':

.. code-block:: python

  get_birth_date = PostgresOperator(
      task_id="get_birth_date",
      postgres_conn_id="postgres_default",
      sql="SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
      parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
  )

Now lets refactor our ``get_birth_date`` task. Instead of dumping SQL statements directly into our code, let's tidy things up
by creating a sql file.

::

  -- dags/sql/birth_date.sql
  SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC {{ params.begin_date }} AND {{ params.end_date }};

And this time we will use the ``params`` attribute which we get for free from the parent ``BaseOperator``
class.

.. code-block:: python

  get_birth_date = PostgresOperator(
      task_id="get_birth_date",
      postgres_conn_id="postgres_default",
      sql="sql/birth_date.sql",
      params={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
  )

Passing Server Configuration Parameters into PostgresOperator
-------------------------------------------------------------

PostgresOperator provides the optional ``runtime_parameters`` attribute which makes it possible to set
the `server configuration parameter values <https://www.postgresql.org/docs/current/runtime-config-client.html>`_ for the SQL request during runtime.

.. exampleinclude:: /../../tests/system/providers/postgres/example_postgres.py
    :language: python
    :start-after: [START postgres_operator_howto_guide_get_birth_date]
    :end-before: [END postgres_operator_howto_guide_get_birth_date]


The complete Postgres Operator DAG
----------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../tests/system/providers/postgres/example_postgres.py
    :language: python
    :start-after: [START postgres_operator_howto_guide]
    :end-before: [END postgres_operator_howto_guide]


Conclusion
----------

In this how-to guide we explored the Apache Airflow PostgreOperator. Let's quickly highlight the key takeaways.
In Airflow-2.0, PostgresOperator class now resides in the ``providers`` package. It is best practice to create subdirectory
called ``sql`` in your ``dags`` directory where you can store your sql files. This will make your code more elegant and more
maintainable. And finally, we looked at the different ways you can dynamically pass parameters into our PostgresOperator
tasks using ``parameters`` or ``params`` attribute and how you can control the server configuration parameters by passing
the ``runtime_parameters`` attribute.
