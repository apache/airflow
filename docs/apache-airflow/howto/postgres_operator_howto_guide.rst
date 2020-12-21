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

The purpose of Postgres Operator is to define tasks involving interactions with the PostgreSQL database. In ``Airflow-1.
10.X``, the ``PostgresOperator`` class is in the ``postgres_operator`` module housed in the ``operators`` package.

However, in ``Airflow-2.0``, the ``PostgresOperator`` class now resides at ``airflow.providers.postgres.operator.postgres``.
You will get a deprecation warning if you try to import the ``postgres_operator`` module.

Under the hood, the ``PostgresOperator`` class delegates its heavy lifting to the ``PostgresHook`` class.

Common Database Operations with PostgresOperator
------------------------------------------------

To use the postgres operator to carry out SQL request, two parameters are required: ``sql`` and ``postgres_conn_id``.
These two parameters are eventually fed to the postgres hook object that interacts directly with the postgres database.

Creating a Postgres database table
----------------------------------

The code snippets below are based on Airflow-2.0

.. code-block:: python

    import datetime

    from airflow import DAG
    from airflow.providers.postgres.operator.postgres import PostgresOperator


    default_args = {
                    "start_date": datetime.datetime(2020, 2, 2),
                    "owner": "airflow"
                    }


    with DAG(dag_id="postgres_dag", schedule_interval="@once", default_args=default_args, catchup=False) as dag:
        create_pet_table = PostgresOperator(
                                            "create_pet_table",
                                            postgres_conn_id = "postgres_default",
                                            sql = """
                                            CREATE TABLE IF NOT EXISTS pet (
                                                  pet_id SERIAL PRIMARY KEY,
                                                  name VARCHAR NOT NULL,
                                                  pet_type VARCHAR NOT NULL,
                                                  birth_date DATE NOT NULL,
                                                  OWNER VARCHAR NOT NULL);
                                                  """
                                            )


Dumping SQL statements into your PostgresOperator isn't quite appealing and will create maintainability pains somewhere
down to the road. To prevent this, Airflow offers an elegant solution for this. This is how it works: you simply create
a directory in the DAG folder called ``sql`` and then put all the SQL files containing your SQL queries.

Your ``dags/sql/pet_schema.sql`` should like this:

.. code-block:: sql

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
                                            "create_pet_table",
                                            postgres_conn_id = "postgres_default",
                                            sql = "sql/pet_schema.sql"
                                            )


Inserting data into a Postgres database table
---------------------------------------------

Let's say we already the insert SQL statement below in our ``dags/sql/pet_schema.sql`` file:

.. code-block:: sql

  -- populate pet table
  INSERT INTO pet VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
  INSERT INTO pet VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
  INSERT INTO pet VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
  INSERT INTO pet VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');

We can then create a PostgresOperator task that populate the ``pet`` table like so:

.. code-block:: python

  populate_pet_table = PostgresOperator(
                                        "populate_pet_table",
                                        postgres_conn_id = "postgres_default",
                                        sql = "sql/pet_schema.sql"
                                        )


Fetching records from your postgres database table
--------------------------------------------------

Fetching records from your postgres database table can be as simple as:

.. code-block:: python

  get_pets = PostgresOperator(
                              "get_pets",
                              postgres_conn_id = "postgres_default",
                              sql = "SELECT * FROM pet;"
                              )



Passing Parameters into your PostgresOperator
---------------------------------------------

PostgresOperator provides ``parameters`` attribute which makes it possible to dynamically inject values into your
SQL requests during runtime. The BaseOperator class has the ``params`` attribute which is available to the PostgresOperator
by virtue of inheritance. Both ``parameters`` and ``params`` make it possible to dynamically pass in parameters in many
interesting ways.

To find the owner of the pet called 'Lester':

.. code-block:: python

  get_birth_date = PostgresOperator(
                                "populate_pet_table",
                                postgres_conn_id = "postgres_default",
                                sql = "SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC %(start_date)s AND %(end_date)s",
                                parameters = {
                                              'start_date': '2020-01-01',
                                              'end_date': '2020-12-31'
                                              }
                                )

Now lets refactor our ``get_birth_date`` task. Instead of dumping SQL statements directly into our code, tidy things up
by creating a sql file.

.. code-block:: sql

  -- dags/sql/birth_date.sql
  SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC {{ params.start_date }} AND {{ params.end_date }};

This time we will use the ``params`` attribute which we get for free from the ``BaseOperator``
class.

.. code-block:: python

  get_birth_date = PostgresOperator(
                                "populate_pet_table",
                                postgres_conn_id = "postgres_default",
                                sql = "sql/birth_date.sql",
                                params = {
                                           'start_date': '2020-01-01',
                                            'end_date': '2020-12-31'
                                          }
                                )


Conclusion
----------

In this how-to guide we have explored the Apache Airflow PostgreOperator. I will quickly highlight the key takeaways.
In Airflow-2.0, PostgresOperator class now resides in the ``providers`` package. It is best practice to create subdirectory
called ``sql`` in your ``dags`` directory where you can store your sql files. This will make your code more elegant and more
maintainable. And finally, we looked at the different ways you can dynamically pass in parameters using ``parameters`` or
``params`` attribute.
