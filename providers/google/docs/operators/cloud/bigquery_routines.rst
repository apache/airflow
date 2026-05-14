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


Google Cloud BigQuery Routines Operators
========================================

`BigQuery routines <https://cloud.google.com/bigquery/docs/routines>`__ are
dataset-scoped resources that encapsulate logic you can reuse from SQL:

* **Scalar user-defined functions** (SQL or JavaScript)
* **Stored procedures** (SQL or Apache Spark)
* **Table-valued functions** (SQL)
* **User-defined aggregate functions** (SQL)
* **Remote functions** backed by Cloud Run / Cloud Functions

Airflow exposes the BigQuery routines API so your DAG can own both the routine
definitions and the pipeline that depends on them, instead of embedding
``CREATE FUNCTION`` / ``CREATE PROCEDURE`` DDL in a query job.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:BigQueryCreateRoutineOperator:

Create a routine
^^^^^^^^^^^^^^^^

Use :class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryCreateRoutineOperator`
to create any routine type. Routine fields mirror the BigQuery REST API's
``Routine`` resource. Pass them individually as keyword arguments, or pass the
complete resource via ``routine_resource``.

The ``if_exists`` argument controls collision behavior:

* ``"fail"`` (default) — raise when the routine already exists.
* ``"skip"`` — leave the existing routine in place and return it.
* ``"replace"`` — delete the existing routine, then create the new one.

Scalar SQL UDF:

.. exampleinclude:: /../../google/tests/system/google/cloud/bigquery/example_bigquery_routines.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_create_scalar_routine]
    :end-before: [END howto_operator_bigquery_create_scalar_routine]

Stored procedure:

.. exampleinclude:: /../../google/tests/system/google/cloud/bigquery/example_bigquery_routines.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_create_procedure_routine]
    :end-before: [END howto_operator_bigquery_create_procedure_routine]

Table-valued function:

.. exampleinclude:: /../../google/tests/system/google/cloud/bigquery/example_bigquery_routines.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_create_tvf_routine]
    :end-before: [END howto_operator_bigquery_create_tvf_routine]

.. _howto/operator:BigQueryUpdateRoutineOperator:

Update a routine
^^^^^^^^^^^^^^^^

Use :class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryUpdateRoutineOperator`
to patch selected fields of an existing routine. Only the fields listed in
``fields`` are updated; any listed field that is unset in ``routine_resource``
is cleared on the server.

.. exampleinclude:: /../../google/tests/system/google/cloud/bigquery/example_bigquery_routines.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_update_routine]
    :end-before: [END howto_operator_bigquery_update_routine]

.. _howto/operator:BigQueryGetRoutineOperator:

Fetch a routine
^^^^^^^^^^^^^^^

Use :class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryGetRoutineOperator`
to read a routine's metadata. The operator pushes the serialized resource to
XCom.

.. exampleinclude:: /../../google/tests/system/google/cloud/bigquery/example_bigquery_routines.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_get_routine]
    :end-before: [END howto_operator_bigquery_get_routine]

.. _howto/operator:BigQueryListRoutinesOperator:

List routines
^^^^^^^^^^^^^

Use :class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryListRoutinesOperator`
to list all routines in a dataset. Only a subset of each routine's fields is
returned; use ``BigQueryGetRoutineOperator`` for the complete resource.

.. exampleinclude:: /../../google/tests/system/google/cloud/bigquery/example_bigquery_routines.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_list_routines]
    :end-before: [END howto_operator_bigquery_list_routines]

.. _howto/operator:BigQueryDeleteRoutineOperator:

Delete a routine
^^^^^^^^^^^^^^^^

Use :class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteRoutineOperator`
to remove a routine. Set ``ignore_if_missing=True`` to make the delete a no-op
when the routine does not exist.

.. exampleinclude:: /../../google/tests/system/google/cloud/bigquery/example_bigquery_routines.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_delete_routine]
    :end-before: [END howto_operator_bigquery_delete_routine]

.. _howto/sensor:BigQueryRoutineExistenceSensor:

Wait for a routine
^^^^^^^^^^^^^^^^^^

Use :class:`~airflow.providers.google.cloud.sensors.bigquery.BigQueryRoutineExistenceSensor`
to block downstream tasks until a routine exists. This is useful when routine
creation happens in a separate DAG or an external system.

.. exampleinclude:: /../../google/tests/system/google/cloud/bigquery/example_bigquery_routines.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_bigquery_routine_existence]
    :end-before: [END howto_sensor_bigquery_routine_existence]

Reference
^^^^^^^^^

For further information, look at:

* `Google Cloud API Documentation <https://cloud.google.com/bigquery/docs/reference/rest/v2/routines>`__
* `User-defined functions <https://cloud.google.com/bigquery/docs/user-defined-functions>`__
* `Stored procedures <https://cloud.google.com/bigquery/docs/procedures>`__
* `Table functions <https://cloud.google.com/bigquery/docs/table-functions>`__
