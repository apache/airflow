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

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:GlueCrawlerOperator:

Create an AWS Glue crawler
==========================

AWS Glue Crawlers allow you to easily extract data from various data sources.
To create a new AWS Glue Crawler or run an existing one you can
use :class:`~airflow.providers.amazon.aws.operators.glue_crawler.GlueCrawlerOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_glue.py
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

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue]
    :end-before: [END howto_operator_glue]

.. note::
  The same AWS IAM role used for the crawler can be used here as well, but it will need
  policies to provide access to the output location for result data.

.. _howto/operator:GlueDataQualityOperator:

Create an AWS Glue Data Quality
===============================

AWS Glue Data Quality allows you to measure and monitor the quality
of your data so that you can make good business decisions.
To create a new AWS Glue Data Quality ruleset or update an existing one you can
use :class:`~airflow.providers.amazon.aws.operators.glue.GlueDataQualityOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_glue_data_quality.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_data_quality_operator]
    :end-before: [END howto_operator_glue_data_quality_operator]

.. _howto/operator:GlueDataQualityRuleSetEvaluationRunOperator:

Start a AWS Glue Data Quality Evaluation Run
=============================================

To start a AWS Glue Data Quality ruleset evaluation run you can use
:class:`~airflow.providers.amazon.aws.operators.glue.GlueDataQualityRuleSetEvaluationRunOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_glue_data_quality.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_data_quality_ruleset_evaluation_run_operator]
    :end-before: [END howto_operator_glue_data_quality_ruleset_evaluation_run_operator]

.. _howto/operator:GlueDataQualityRuleRecommendationRunOperator:

Start a AWS Glue Data Quality Recommendation Run
=================================================

To start a AWS Glue Data Quality rule recommendation run you can use
:class:`~airflow.providers.amazon.aws.operators.glue.GlueDataQualityRuleRecommendationRunOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_glue_data_quality_with_recommendation.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_data_quality_rule_recommendation_run]
    :end-before: [END howto_operator_glue_data_quality_rule_recommendation_run]

Sensors
-------

.. _howto/sensor:GlueCrawlerSensor:

Wait on an AWS Glue crawler state
=================================

To wait on the state of an AWS Glue crawler execution until it reaches a terminal state you can
use :class:`~airflow.providers.amazon.aws.sensors.glue_crawler.GlueCrawlerSensor`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_glue_crawler]
    :end-before: [END howto_sensor_glue_crawler]

.. _howto/sensor:GlueJobSensor:

Wait on an AWS Glue job state
=============================

To wait on the state of an AWS Glue Job until it reaches a terminal state you can
use :class:`~airflow.providers.amazon.aws.sensors.glue.GlueJobSensor`

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_glue]
    :end-before: [END howto_sensor_glue]

.. _howto/sensor:GlueDataQualityRuleSetEvaluationRunSensor:

Wait on an AWS Glue Data Quality Evaluation Run
================================================

To wait on the state of an AWS Glue Data Quality RuleSet Evaluation Run until it
reaches a terminal state you can use :class:`~airflow.providers.amazon.aws.sensors.glue.GlueDataQualityRuleSetEvaluationRunSensor`

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_glue_data_quality.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_glue_data_quality_ruleset_evaluation_run]
    :end-before: [END howto_sensor_glue_data_quality_ruleset_evaluation_run]

.. _howto/sensor:GlueDataQualityRuleRecommendationRunSensor:

Wait on an AWS Glue Data Quality Recommendation Run
====================================================

To wait on the state of an AWS Glue Data Quality recommendation run until it
reaches a terminal state you can use :class:`~airflow.providers.amazon.aws.sensors.glue.GlueDataQualityRuleRecommendationRunSensor`

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_glue_data_quality_with_recommendation.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_glue_data_quality_rule_recommendation_run]
    :end-before: [END howto_sensor_glue_data_quality_rule_recommendation_run]

.. _howto/sensor:GlueCatalogPartitionSensor:

Wait on an AWS Glue Catalog Partition
======================================

To wait for a partition to show up in AWS Glue Catalog until it
reaches a terminal state you can use :class:`~airflow.providers.amazon.aws.sensors.glue_catalog_partition.GlueCatalogPartitionSensor`

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_glue_catalog_partition]
    :end-before: [END howto_sensor_glue_catalog_partition]

Reference
---------

* `AWS boto3 library documentation for Glue <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html>`__
* `Glue IAM Role creation <https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html>`__
* `AWS boto3 library documentation for Glue DataBrew <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/databrew.html>`__
