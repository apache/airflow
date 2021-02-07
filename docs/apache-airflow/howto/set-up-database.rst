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



Set up a Database Backend
=========================

Airflow was built to interact with its metadata using `SqlAlchemy <https://docs.sqlalchemy.org/en/13/>`__.

The document below describes the database engine configurations, the necessary changes to their configuration to be used with Airflow, as well as changes to the Airflow configurations to connect to these databases.

Choosing database backend
-------------------------

If you want to take a real test drive of Airflow, you should consider setting up a database backend to **MySQL** and **PostgresSQL**.
By default, Airflow uses **SQLite**, which is not intended for development purposes only.

Airflow supports the following database engine versions, so make sure which version you have. Old versions may not support all SQL statements.

  * PostgreSQL:  9.6, 10, 11, 12, 13
  * MySQL: 5.7, 8
  * SQLite: 3.15.0+

If you plan on running more than one scheduler, you have to meet additional requirements.
For details, see :ref:`Scheduler HA Database Requirements <scheduler:ha:db_requirements>`.

Database URI
------------

Airflow uses SQLAlchemy to connect to the database, which requires you to configure the Database URL.
You can do this in option ``sql_alchemy_conn`` in section ``[core]``. It is also common to configure
this option with ``AIRFLOW__CORE__SQL_ALCHEMY_CONN`` environment variable.

.. note::
    For more information on setting the configuration, see :doc:`/howto/set-config`.

If you want to check the current value, you can use ``airflow config get-value core sql_alchemy_conn`` command as in
the example below.

.. code-block:: bash

    $ airflow config get-value core sql_alchemy_conn
    sqlite:////tmp/airflow/airflow.db

The exact format description is described in the SQLAlchemy documentation, see `Database Urls <https://docs.sqlalchemy.org/en/14/core/engines.html>`__. We will also show you some examples below.

Setting up a MySQL Database
---------------------------

You need to create a database and a database user that Airflow will use to access this database.
In the example below, a database ``airflow_db`` and user  with username ``airflow_user`` with password ``airflow_pass`` will be created

.. code-block:: sql

   CREATE DATABASE airflow_db CHARACTER SET utf8 COLLATE utf8_unicode_ci;
   CREATE USER 'airflow_user' IDENTIFIED BY 'airflow_pass';
   GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow_user';

We rely on more strict ANSI SQL settings for MySQL in order to have sane defaults.
Make sure to have specified ``explicit_defaults_for_timestamp=1`` option under ``[mysqld]`` section
in your ``my.cnf`` file. You can also activate these options with the ``--explicit-defaults-for-timestamp`` switch passed to ``mysqld`` executable

We recommend using the ``mysqlclient`` driver and specifying it in your SqlAlchemy connection string.

.. code-block:: text

    mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>

But we also support the ``mysql-connector-python`` driver, which lets you connect through SSL
without any cert options provided.

.. code-block:: text

   mysql+mysqlconnector://<user>:<password>@<host>[:<port>]/<dbname>

However if you want to use other drivers visit the `MySQL Dialect <https://docs.sqlalchemy.org/en/13/dialects/mysql.html>`__  in SQLAlchemy documentation for more information regarding download
and setup of the SqlAlchemy connection.

Setting up a PostgreSQL Database
--------------------------------

You need to create a database and a database user that Airflow will use to access this database.
In the example below, a database ``airflow_db`` and user  with username ``airflow_user`` with password ``airflow_pass`` will be created

.. code-block:: sql

   CREATE DATABASE airflow_db;
   CREATE USER airflow_user WITH PASSWORD 'airflow_user';
   GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;

You may need to update your Postgres ``pg_hba.conf`` to add the
``airflow`` user to the database access control list; and to reload
the database configuration to load your change. See
`The pg_hba.conf File <https://www.postgresql.org/docs/current/auth-pg-hba-conf.html>`__
in the Postgres documentation to learn more.

We recommend using the ``psycopg2`` driver and specifying it in your SqlAlchemy connection string.

.. code-block:: text

   postgresql+psycopg2://<user>:<password>@<host>/<db>

Also note that since SqlAlchemy does not expose a way to target a specific schema in the database URI, you may
want to set a default schema for your role with a SQL statement similar to ``ALTER ROLE username SET search_path = airflow, foobar;``

For more information regarding setup of the PostgresSQL connection, see `PostgreSQL dialect <https://docs.sqlalchemy.org/en/13/dialects/postgresql.html>`__ in SQLAlchemy documentation.

.. spelling::

     hba

Other configuration options
---------------------------

There are more configuration options for configuring SQLAlchemy behavior. For details, see :ref:`reference documentation <config:core>` for ``sqlalchemy_*`` option in ``[core]`` section.

Initialize the database
-----------------------

After configuring the database and connecting to it in Airflow configuration, you should create the database schema.

.. code-block:: bash

    airflow db init

What's next?
------------

By default, Airflow uses ``SequentialExecutor``, which does not provide parallelism. You should consider
configuring a different :doc:`executor </executor/index>` for better performance.
