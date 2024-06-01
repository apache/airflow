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

.. _howto/operators:ydb:

How-to Guide for YDB using YDBOperator
=======================================================

Introduction
------------

Apache Airflow has a robust trove of operators that can be used to implement the various tasks that make up your
workflow. Airflow is essentially a graph (Directed Acyclic Graph) made up of tasks (nodes) and dependencies (edges).

A task defined or implemented by a operator is a unit of work in your data pipeline.

The purpose of this guide is to define tasks involving interactions with a YDB database with
the :class:`~airflow.providers.ydb.operators.YDBOperator`.

Common Database Operations with YDBOperator
-------------------------------------------------------

To use the YDBOperator to carry out YDBOperator request, two parameters are required: ``sql`` and ``conn_id``.
These two parameters are eventually fed to the DbApiHook object that interacts directly with the YDB database.

Creating a YDB table
----------------------------------

The code snippets below are based on Airflow-2.0

.. exampleinclude:: /../../tests/system/providers/ydb/example_ydb.py
    :language: python
    :start-after: [START ydb_sql_execute_query_operator_howto_guide]
    :end-before: [END ydb_sql_execute_query_operator_howto_guide_create_pet_table]


Dumping SQL statements into your operator isn't quite appealing and will create maintainability pains somewhere
down to the road. To prevent this, Airflow offers an elegant solution. This is how it works: you simply create
a directory inside the DAG folder called ``sql`` and then put all the SQL files containing your SQL queries inside it.

Your ``dags/sql/pet_schema.sql`` should like this:

::

      -- create pet table
      CREATE TABLE pet (
        pet_id INT,
        name TEXT NOT NULL,
        pet_type TEXT NOT NULL,
        birth_date TEXT NOT NULL,
        owner TEXT NOT NULL,
        PRIMARY KEY (pet_id)
      );

Now let's refactor ``create_pet_table`` in our DAG:

.. code-block:: python

        create_pet_table = YDBOperator(
            task_id="create_pet_table",
            conn_id="ydb_default",
            sql="sql/pet_schema.sql",
        )


Inserting data into an YDB table
---------------------------------------------

Let's say we already have the SQL insert statement below in our ``dags/sql/pet_schema.sql`` file:

::

  -- populate pet table
  UPSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
  VALUES ( 1, 'Max', 'Dog', '2018-07-05', 'Jane');

  UPSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
  VALUES ( 2, 'Susie', 'Cat', '2019-05-01', 'Phil');

  UPSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
  VALUES ( 3, 'Lester', 'Hamster', '2020-06-23', 'Lily');

  UPSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
  VALUES ( 4, 'Quincy', 'Parrot', '2013-08-11', 'Anne');

We can then create a YDBOperator task that populate the ``pet`` table.

.. code-block:: python

  populate_pet_table = YDBOperator(
      task_id="populate_pet_table",
      conn_id="ydb_default",
      sql="sql/pet_schema.sql",
  )


Fetching records from your YDB table
--------------------------------------------------

Fetching records from your YDB table can be as simple as:

.. code-block:: python

  get_all_pets = YDBOperator(
      task_id="get_all_pets",
      conn_id="ydb_default",
      sql="SELECT * FROM pet;",
  )


Passing Parameters into YDBOperator
------------------------------------------------------------

YDBOperator provides ``parameters`` attribute which makes it possible to dynamically inject values into your
SQL requests during runtime. The BaseOperator class has the ``params`` attribute which is available to the YDBOperator
by virtue of inheritance. Both ``parameters`` and ``params`` make it possible to dynamically pass in parameters in many
interesting ways.

To find the owner of the pet called 'Lester':

.. code-block:: python

  get_birth_date = YDBOperator(
      task_id="get_birth_date",
      conn_id="ydb_default",
      sql="SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
      parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
  )

Now lets refactor our ``get_birth_date`` task. Instead of dumping SQL statements directly into our code, let's tidy things up
by creating a sql file.

::

  -- dags/sql/birth_date.sql
  SELECT * FROM pet WHERE birth_date BETWEEN {{ params.begin_date }} AND {{ params.end_date }};

And this time we will use the ``params`` attribute which we get for free from the parent ``BaseOperator``
class.

.. code-block:: python

  get_birth_date = YDBOperator(
      task_id="get_birth_date",
      conn_id="ydb_default",
      sql="sql/birth_date.sql",
      params={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
  )


The complete YDB Operator DAG
----------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../tests/system/providers/ydb/example_ydb.py
    :language: python
    :start-after: [START ydb_sql_execute_query_operator_howto_guide]
    :end-before: [END ydb_sql_execute_query_operator_howto_guide]


Conclusion
----------

In this how-to guide we explored the Apache Airflow YDBOperator to connect to YDB database. Let's quickly highlight the key takeaways.
It is best practice to create subdirectory called ``sql`` in your ``dags`` directory where you can store your sql files.
This will make your code more elegant and more maintainable.
And finally, we looked at the different ways you can dynamically pass parameters into our YDBOperator
tasks using ``parameters`` or ``params`` attribute and how you can control the session parameters by passing
options in the ``hook_params`` attribute.
