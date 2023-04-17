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

=================
Amazon QuickSight
=================

`Amazon QuickSight <https://aws.amazon.com/quicksight/>`__ is a fast business analytics service
to build visualizations, perform ad hoc analysis, and quickly get business insights from your data.
Amazon QuickSight seamlessly discovers AWS data sources, enables organizations to scale to hundreds
of thousands of users, and delivers fast and responsive query performance by using the Amazon
QuickSight Super-fast, Parallel, In-Memory, Calculation Engine (SPICE).

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:QuickSightCreateIngestionOperator:

Amazon QuickSight create ingestion
==================================

The ``QuickSightCreateIngestionOperator`` creates and starts a new SPICE ingestion for a dataset.
The operator also refreshes existing SPICE datasets.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_quicksight.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_quicksight_create_ingestion]
    :end-before: [END howto_operator_quicksight_create_ingestion]

Sensors
-------

.. _howto/sensor:QuickSightSensor:

Amazon QuickSight ingestion sensor
==================================

The ``QuickSightSensor`` waits for an Amazon QuickSight create ingestion until it reaches a terminal state.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_quicksight.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_quicksight]
    :end-before: [END howto_sensor_quicksight]

Reference
---------

* `Boto3 Library Documentation for QuickSight <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/quicksight.html>`__
