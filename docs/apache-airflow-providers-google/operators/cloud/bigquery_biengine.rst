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

Google Cloud BigQuery Reservation Operators
=====================================================

The `BigQuery BI Engine <https://cloud.google.com/bigquery/docs/bi-engine-intro>`__
is a fast, in-memory analysis service that accelerates many SQL queries in BigQuery
by intelligently caching the data you use most frequently.


Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:BigQueryBiEngineReservationCreateOperator:

Creating BI Engine reservation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can create the operator with or without project id. If project id is missing
it will be retrieved from the Google Cloud connection used.

To create BI Engine reservation you can use
:class:`~airflow.providers.google.cloud.operators.bigquery_biengine.BigQueryBiEngineReservationCreateOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/bigquery/example_bigquery_reservation_bi_engine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_create_bi_engine_reservation]
    :end-before: [END howto_operator_bigquery_create_bi_engine_reservation]


.. _howto/operator:BigQueryBiEngineReservationDeleteOperator:

Deleting BI Engine reservation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To delete BI Engine reservation you can use
:class:`~airflow.providers.google.cloud.operators.bigquery_biengine.BigQueryBiEngineReservationDeleteOperator`.

You can create the operator with or without project id and size. If project id is missing
it will be retrieved from the Google Cloud connection used. If size is missing, all reservations will be deleted.

Basic usage of the operator:

.. exampleinclude:: /../../tests/system/providers/google/cloud/bigquery/example_bigquery_reservation_bi_engine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_delete_bi_engine_reservation]
    :end-before: [END howto_operator_bigquery_delete_bi_engine_reservation]


Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://cloud.google.com/python/docs/reference/bigqueryreservation/latest>`__
* `Product Documentation <https://cloud.google.com/bigquery/>`__
