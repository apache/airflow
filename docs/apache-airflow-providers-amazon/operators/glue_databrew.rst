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
AWS Glue DataBrew
=================

`AWS Glue DataBrew <https://aws.amazon.com/glue/features/databrew/>`__ is a visual data preparation tool
that makes it easier for data analysts and data scientists to clean and normalize data to prepare it
for analytics and machine learning (ML). You can choose from over 250 prebuilt transformations to automate
data preparation tasks, all without the need to write any code. You can automate filtering anomalies, converting
data to standard formats and correcting invalid values, and other tasks.
After your data is ready, you can immediately use it for analytics and ML projects.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:GlueDataBrewStartJobOperator:

Start an AWS Glue DataBrew job
==============================

To submit a new AWS Glue DataBrew job you can use :class:`~airflow.providers.amazon.aws.operators.glue_databrew.GlueDataBrewStartJobOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_glue_databrew.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_databrew_start]
    :end-before: [END howto_operator_glue_databrew_start]

Reference
---------

* `AWS boto3 library documentation for Glue DataBrew <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/databrew.html>`__
