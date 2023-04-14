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

========
AWS Glue
========

`AWS Glue <https://aws.amazon.com/glue/>`__ is a serverless data integration service that makes it
easy to discover, prepare, and combine data for analytics, machine learning, and application development.
AWS Glue provides all the capabilities needed for data integration so that you can start analyzing
your data and putting it to use in minutes instead of months.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:GlueCrawlerOperator:

Create an AWS Glue crawler
==========================

AWS Glue Crawlers allow you to easily extract data from various data sources.
To create a new AWS Glue Crawler or run an existing one you can
use :class:`~airflow.providers.amazon.aws.operators.glue_crawler.GlueCrawlerOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_crawler]
    :end-before: [END howto_operator_glue_crawler]

.. note::
  The AWS IAM role included in the ``config`` needs access to the source data location
  (e.g. s3:PutObject access if data is stored in Amazon S3) as well as the ``AWSGlueServiceRole``
  policy. See the References section below for a link to more details.

.. _howto/operator:GlueJobOperator:

Submit an AWS Glue job
======================

To submit a new AWS Glue job you can use :class:`~airflow.providers.amazon.aws.operators.glue.GlueJobOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue]
    :end-before: [END howto_operator_glue]

.. note::
  The same AWS IAM role used for the crawler can be used here as well, but it will need
  policies to provide access to the output location for result data.

Sensors
-------

.. _howto/sensor:GlueCrawlerSensor:

Wait on an AWS Glue crawler state
=================================

To wait on the state of an AWS Glue crawler execution until it reaches a terminal state you can
use :class:`~airflow.providers.amazon.aws.sensors.glue_crawler.GlueCrawlerSensor`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_glue_crawler]
    :end-before: [END howto_sensor_glue_crawler]

.. _howto/sensor:GlueJobSensor:

Wait on an AWS Glue job state
=============================

To wait on the state of an AWS Glue Job until it reaches a terminal state you can
use :class:`~airflow.providers.amazon.aws.sensors.glue.GlueJobSensor`

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_glue]
    :end-before: [END howto_sensor_glue]

Reference
---------

* `AWS boto3 library documentation for Glue <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html>`__
* `Glue IAM Role creation <https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html>`__
