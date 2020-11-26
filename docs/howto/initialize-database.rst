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



Initializing a Database Backend
===============================

If you want to take a real test drive of Airflow, you should consider
setting up a real database backend and switching to the LocalExecutor.

Airflow was built to interact with its metadata using SqlAlchemy
with **MySQL**,  **Postgres** and **SQLite** as supported backends (SQLite is used primarily for development purpose).

.. seealso:: :ref:`Scheduler HA Database Requirements <scheduler:ha:db_requirements>` if you plan on running
   more than one scheduler

.. note:: We rely on more strict ANSI SQL settings for MySQL in order to have
   sane defaults. Make sure to have specified ``explicit_defaults_for_timestamp=1``
   in your my.cnf under ``[mysqld]``

.. note:: If you decide to use **MySQL**, we recommend using the ``mysqlclient``
   driver and specifying it in your SqlAlchemy connection string. (I.e.,
   ``mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>``.)
   But we also support the ``mysql-connector-python`` driver (I.e.,
   ``mysql+mysqlconnector://<user>:<password>@<host>[:<port>]/<dbname>``.) which lets you connect through SSL
   without any cert options provided. However if you want to use other drivers visit the
   `SqlAlchemy docs <https://docs.sqlalchemy.org/en/13/dialects/mysql.html>`_ for more information regarding download
   and setup of the SqlAlchemy connection.

.. note:: If you decide to use **Postgres**, we recommend using the ``psycopg2``
   driver and specifying it in your SqlAlchemy connection string. (I.e.,
   ``postgresql+psycopg2://<user>:<password>@<host>/<db>``.)
   Also note that since SqlAlchemy does not expose a way to target a
   specific schema in the Postgres connection URI, you may
   want to set a default schema for your role with a
   command similar to ``ALTER ROLE username SET search_path = airflow, foobar;``

Setup your database to host Airflow
-----------------------------------

Create a database called ``airflow`` and a database user that Airflow
will use to access this database.

Example, for **MySQL**:

.. code-block:: sql

   CREATE DATABASE airflow CHARACTER SET utf8 COLLATE utf8_unicode_ci;
   CREATE USER 'airflow' IDENTIFIED BY 'airflow';
   GRANT ALL PRIVILEGES ON airflow.* TO 'airflow';

Example, for **Postgres**:

.. code-block:: sql

   CREATE DATABASE airflow;
   CREATE USER airflow WITH PASSWORD 'airflow';
   GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

You may need to update your Postgres ``pg_hba.conf`` to add the
``airflow`` user to the database access control list; and to reload
the database configuration to load your change. See
`The pg_hba.conf File <https://www.postgresql.org/docs/current/auth-pg-hba-conf.html>`__
in the Postgres documentation to learn more.

Configure Airflow's database connection string
----------------------------------------------

Once you have setup your database to host Airflow, you'll need to alter the
SqlAlchemy connection string located in ``sql_alchemy_conn`` option in ``[core]`` section in your configuration file
``$AIRFLOW_HOME/airflow.cfg``.

You can also define connection URI using ``AIRFLOW__CORE__SQL_ALCHEMY_CONN`` environment variable.

Configure a worker that supports parallelism
--------------------------------------------

You should then also change the ``executor`` option in the ``[core]`` option to use ``LocalExecutor``, an executor that can parallelize task instances locally.

Initialize the database
-----------------------

.. code-block:: bash

    # initialize the database
    airflow db init

.. spelling::

     hba
