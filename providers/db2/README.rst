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

``apache-airflow-providers-db2``
=================================

Provider package for IBM Db2 database support in Apache Airflow.

Installation
------------

You can install this package via pip:

.. code-block:: bash

    pip install apache-airflow-providers-db2

Requirements
------------

This provider requires the following packages:

- ``ibm-db>=3.0.0`` - IBM Db2 Python driver
- ``ibm-db-sa>=0.4.0`` - SQLAlchemy dialect for Db2

Configuration
-------------

To use this provider, you need to configure a Db2 connection in Airflow:

1. In the Airflow UI, go to Admin -> Connections
2. Create a new connection with the following details:

   - **Connection ID**: ``db2_default`` (or any custom name)
   - **Connection Type**: ``IBM Db2``
   - **Host**: Your Db2 server hostname
   - **Schema**: Database name
   - **Login**: Db2 username
   - **Password**: Db2 password
   - **Port**: Db2 port (default: 50000)
   - **Extra**: Optional JSON with additional parameters:

     .. code-block:: json

        {
          "ssl": true,
          "sslcert": "/path/to/cert.pem"
        }

Usage
-----

Using the Db2Hook
~~~~~~~~~~~~~~~~~

.. code-block:: python

    from airflow.providers.db2.hooks.db2 import Db2Hook

    hook = Db2Hook(db2_conn_id="db2_default")
    records = hook.get_records("SELECT * FROM employees WHERE dept = %s", parameters=("IT",))

Using with SQLExecuteQueryOperator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from airflow import DAG
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    from datetime import datetime

    with DAG("db2_example", start_date=datetime(2024, 1, 1)) as dag:
        query_task = SQLExecuteQueryOperator(
            task_id="query_db2",
            conn_id="db2_default",
            sql="SELECT * FROM employees WHERE dept = %(dept)s",
            parameters={"dept": "IT"},
        )

Features
--------

- Full Db2 database connectivity via ``ibm_db_dbi``
- Support for SSL connections
- Autocommit and executemany support

License
-------

Apache License 2.0
