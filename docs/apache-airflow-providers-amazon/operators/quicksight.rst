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


Amazon QuickSight Operators
========================================

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Overview
--------

Airflow to Amazon QuickSight integration allows to create and start the SPICE ingestion for dataset.

  - :class:`~airflow.providers.amazon.aws.operators.quicksight.QuickSightCreateIngestionOperator`

Purpose
"""""""

This example DAG ``example_quicksight.py`` uses ``QuickSightCreateIngestionOperator`` for
creating and starting the SPICE ingestion for the dataset configured to use SPICE

Defining tasks
""""""""""""""

In the following code we create and start a QuickSight SPICE ingestion for the dataset.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_quicksight.py
    :language: python
    :start-after: [START howto_operator_quicksight]
    :end-before: [END howto_operator_quicksight]

Reference
---------

For further information, look at:

* `Boto3 Library Documentation for QuickSight <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/quicksight.html>`__
