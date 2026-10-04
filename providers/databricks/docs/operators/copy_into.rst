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

.. _howto/operator:DatabricksSqlCopyIntoOperator:


DatabricksCopyIntoOperator
==========================

Use the :class:`~airflow.providers.databricks.operators.databricks_sql.DatabricksCopyIntoOperator` to import
data into Databricks table using `COPY INTO <https://docs.databricks.com/sql/language-manual/delta-copy-into.html>`_
command.


Using the Operator
------------------

Operator loads data from a specified location into a table using a configured endpoint.  The only required parameters are:

* ``table_name`` - string with the table name
* ``file_location`` - string with the URI of data to load
* ``file_format`` - string specifying the file format of data to load. Supported formats are ``CSV``, ``JSON``, ``AVRO``, ``ORC``, ``PARQUET``, ``TEXT``, ``BINARYFILE``.
* One of ``sql_endpoint_name`` (name of Databricks SQL endpoint to use) or ``http_path`` (HTTP path for Databricks SQL endpoint or Databricks cluster).

Other parameters are optional and could be found in the class documentation.

Examples
--------

Importing CSV data
^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksCopyIntoOperator to import CSV data into a table is as follows:

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_copy_into]
    :end-before: [END howto_operator_databricks_copy_into]

.. _howto/operator:DatabricksCopyIntoAssetOperator:

DatabricksCopyIntoAssetOperator
===============================

Use :class:`~airflow.providers.databricks.operators.databricks_sql.DatabricksCopyIntoAssetOperator`
when the ``COPY INTO`` target is a Unity Catalog table that downstream Dags schedule on.
It accepts every ``DatabricksCopyIntoOperator`` argument plus a required ``unity_table``.
``DatabricksCopyIntoOperator`` itself declares no assets.

``unity_table`` is a :class:`~airflow.providers.databricks.assets.databricks.UnityTableIdentity`
with ``host``, ``catalog``, ``schema``, and ``table``. All four are required and static.
Jinja in any field raises ``ValueError``. ``host`` follows the Databricks connection rule, so
``https://my-workspace.cloud.databricks.com/`` becomes ``my-workspace.cloud.databricks.com``.
``unity_table.to_asset()`` returns the ``databricks://host/catalog/schema/table`` asset.

.. code-block:: python

    from airflow.providers.databricks.assets.databricks import UnityTableIdentity
    from airflow.providers.databricks.operators.databricks_sql import DatabricksCopyIntoAssetOperator

    users = UnityTableIdentity(
        host="my-workspace.cloud.databricks.com",
        catalog="main",
        schema="default",
        table="users",
    )

    load_users = DatabricksCopyIntoAssetOperator(
        task_id="load_users",
        sql_endpoint_name="my-endpoint",
        file_location="/Volumes/main/default/landing/users.csv",
        file_format="CSV",
        table_name="main.default.users",
        unity_table=users,
    )

Outlets
-------

When you omit ``outlets``, the operator sets ``outlets=[unity_table.to_asset()]`` at parse time.
When you pass ``outlets``, including ``outlets=[]``, the operator keeps your value.
To add extra outlets, pass the Unity table asset among them, as in
``outlets=[users.to_asset(), other_asset]``.

Templated table names
---------------------

``table_name`` stays templated. ``unity_table`` is not templated and fixes the asset at parse time.
Before running SQL, the operator resolves the rendered ``table_name`` and compares it with ``unity_table``.

* A three-part name ``catalog.schema.table`` is used as is.
* A two-part name ``schema.table`` takes the catalog from the ``catalog`` argument.
* A one-part name ``table`` takes the catalog and schema from the ``catalog`` and ``schema`` arguments.

The operator never guesses the workspace default catalog or schema. If a part is missing or the
resolved table differs from ``unity_table``, the task raises ``ValueError`` and no SQL runs.

Dynamic task mapping
--------------------

A mapped task does not run the operator constructor when the Dag is parsed, so it gets no
automatic outlet. Pass ``outlets=[users.to_asset()]`` in ``partial()``. All mapped instances share
one ``unity_table``, so each rendered ``table_name`` must still resolve to that table.

Sources and other operators
---------------------------

``file_location`` may be a Unity Catalog volume path such as ``/Volumes/main/default/landing``.
The volume is the ``COPY INTO`` source. It is not an outlet. The outlet is the target table.

:class:`~airflow.providers.databricks.operators.databricks.DatabricksSQLStatementsOperator` does not
infer assets from SQL. Pass ``outlets=[users.to_asset()]`` for the tables your statements write.

Databricks job and pipeline operators, such as
:class:`~airflow.providers.databricks.operators.databricks.DatabricksRunNowOperator`, do not infer
table assets either. Pass ``outlets`` explicitly.
