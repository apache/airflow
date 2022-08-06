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


===============================
Amazon EMR Serverless Operators
===============================

`Amazon EMR Serverless <https://aws.amazon.com/emr/serverless/>`__ is a serverless option
in Amazon EMR that makes it easy for data analysts and engineers to run open-source big
data analytics frameworks without configuring, managing, and scaling clusters or servers.
You get all the features and benefits of Amazon EMR without the need for experts to plan
and manage clusters.

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Operators
---------
.. _howto/operator:EmrServerlessCreateApplicationOperator:

Create an EMR Serverless Application
====================================

You can use :class:`~airflow.providers.amazon.aws.operators.emr.EmrServerlessCreateApplicationOperator` to
create a new EMR Serverless Application.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_emr_serverless.py
   :language: python
   :dedent: 4
   :start-after: [START howto_operator_emr_serverless_create_application]
   :end-before: [END howto_operator_emr_serverless_create_application]

.. _howto/operator:EmrServerlessStartJobOperator:

Start an EMR Serverless Job
============================

You can use :class:`~airflow.providers.amazon.aws.operators.emr.EmrServerlessStartJobOperator` to
start an EMR Serverless Job.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_emr_serverless.py
   :language: python
   :dedent: 4
   :start-after: [START howto_operator_emr_serverless_start_job]
   :end-before: [END howto_operator_emr_serverless_start_job]

.. _howto/operator:EmrServerlessDeleteApplicationOperator:

Delete an EMR Serverless Application
====================================

You can use :class:`~airflow.providers.amazon.aws.operators.emr.EmrServerlessDeleteApplicationOperator` to
delete an EMR Serverless Application.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_emr_serverless.py
   :language: python
   :dedent: 4
   :start-after: [START howto_operator_emr_serverless_delete_application]
   :end-before: [END howto_operator_emr_serverless_delete_application]

Sensors
-------

.. _howto/sensor:EmrServerlessJobSensor:

Wait on an EMR Serverless Job state
===================================

To monitor the state of an EMR Serverless Job you can use
:class:`~airflow.providers.amazon.aws.sensors.emr.EmrServerlessJobSensor`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_emr_serverless.py
   :language: python
   :dedent: 4
   :start-after: [START howto_sensor_emr_serverless_job]
   :end-before: [END howto_sensor_emr_serverless_job]

.. _howto/sensor:EmrServerlessApplicationSensor:

Wait on an EMR Serverless Application state
============================================

To monitor the state of an EMR Serverless Application you can use
:class:`~airflow.providers.amazon.aws.sensors.emr.EmrServerlessApplicationSensor`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_emr_serverless.py
   :language: python
   :dedent: 4
   :start-after: [START howto_sensor_emr_serverless_application]
   :end-before: [END howto_sensor_emr_serverless_application]

Reference
---------

* `AWS boto3 library documentation for EMR Serverless <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr-serverless.html>`__
* `Configure IAM Roles for EMR Serverless permissions <https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/getting-started.html>`__
