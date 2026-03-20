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

.. _howto/operator:VespaIngestOperator:

VespaIngestOperator
===================

Use the :class:`~airflow.providers.vespa.operators.vespa_ingest.VespaIngestOperator` to
ingest, update, or delete documents in a Vespa instance.


Using the Operator
^^^^^^^^^^^^^^^^^^

The ``VespaIngestOperator`` accepts a list of documents through the ``docs`` argument and defers the
feed operation to a trigger. Use ``vespa_conn_id`` to specify the Airflow connection to use when
resolving the target Vespa instance.

.. note::

   Because the operator always defers execution, a running
   `triggerer <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/security/workload.html>`__
   component is required.

An example using the operator in this way:

.. exampleinclude:: /../../vespa/tests/system/vespa/example_dag_vespa.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_vespa_ingest]
    :end-before: [END howto_operator_vespa_ingest]
