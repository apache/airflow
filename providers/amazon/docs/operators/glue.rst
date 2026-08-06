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

.. _howto/operator:GlueCrawlerCreateOperator:

Create an AWS Glue crawler
==========================

AWS Glue Crawlers allow you to easily extract data from various data sources.
To create a crawler, use
:class:`~airflow.providers.amazon.aws.operators.glue_crawler.GlueCrawlerCreateOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_crawler_create]
    :end-before: [END howto_operator_glue_crawler_create]

.. note::
  The AWS IAM role included in the ``config`` needs access to the source data location
  (e.g. s3:PutObject access if data is stored in Amazon S3) as well as the ``AWSGlueServiceRole``
  policy. See the References section below for a link to more details.

.. _howto/operator:GlueCrawlerUpdateOperator:

Update an AWS Glue crawler
==========================

To update the configuration of an existing crawler, use
:class:`~airflow.providers.amazon.aws.operators.glue_crawler.GlueCrawlerUpdateOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_crawler_update]
    :end-before: [END howto_operator_glue_crawler_update]

.. _howto/operator:GlueCrawlerRunOperator:

Run an AWS Glue crawler
=======================

To run an existing crawler and wait for it to complete, use
:class:`~airflow.providers.amazon.aws.operators.glue_crawler.GlueCrawlerRunOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_crawler_run]
    :end-before: [END howto_operator_glue_crawler_run]

The operator waits for completion by default. Set ``deferrable=True`` to perform the wait without
occupying a worker slot.

.. _howto/operator:GlueCrawlerDeleteOperator:

Delete an AWS Glue crawler
==========================

To delete an existing crawler, use
:class:`~airflow.providers.amazon.aws.operators.glue_crawler.GlueCrawlerDeleteOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_crawler_delete]
    :end-before: [END howto_operator_glue_crawler_delete]

.. _howto/operator:GlueCrawlerOperator:

Legacy AWS Glue crawler operator
================================

.. warning::
  :class:`~airflow.providers.amazon.aws.operators.glue_crawler.GlueCrawlerOperator` is deprecated.
  Existing Dags can continue using it during the deprecation period, but new Dags should use the
  operation-specific operators above.

The legacy operator creates or updates a crawler and then runs it. Existing Dags can continue using
the same configuration while migrating each operation to the dedicated operators:

.. code-block:: python

    crawl_s3 = GlueCrawlerOperator(
        task_id="crawl_s3",
        config=glue_crawler_config,
    )

.. _howto/operator:GlueJobOperator:

Submit an AWS Glue job
======================

To submit a new AWS Glue job you can use :class:`~airflow.providers.amazon.aws.operators.glue.GlueJobOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue]
    :end-before: [END howto_operator_glue]

.. note::
  The same AWS IAM role used for the crawler can be used here as well, but it will need
  policies to provide access to the output location for result data.

Durable execution
==================

``GlueJobOperator`` submits a job run and then polls it to completion on the worker. By default
the operator runs in a *durable* mode that makes this crash-safe: the Glue job run id is
persisted to :doc:`task state store <apache-airflow:core-concepts/task-state-store>` before
polling begins, so if the worker crashes or is preempted and the task is retried, the operator
reconnects to the run that is already executing in Glue instead of starting a new one.

This matters more for Glue because a Glue job's ``concurrent_run_limit`` defaults to ``1``, so
submitting a second run while the first is still active does not create a harmless duplicate, it
fails outright with ``ConcurrentRunsExceededException`` and the task keeps retrying against a run
it can never see. Durable execution turns that retry into a normal reconnect.

On retry the operator checks the prior run's state:

* if it is still starting, running, waiting for capacity, or being stopped, the operator
  reconnects and continues polling
* if it already succeeded, or was stopped outside Airflow, the operator returns immediately
  without resubmitting
* if it failed terminally, or its id has expired and is no longer found, the operator submits the
  job fresh

A run that was stopped outside Airflow (for example, cancelled manually in the AWS console) is
treated as a success rather than resubmitted, since the work is genuinely finished, just not the
way the task expected - the operator logs a warning when this happens.

This protection also applies when ``wait_for_completion=False`` -- even though that task attempt
never polls at all, a retry after a successful submission still reconnects rather than
resubmitting, since the run id is persisted immediately after submission regardless of whether the
task waits for it to finish.

Durable execution requires Airflow 3.3 or newer for the task state store lookup above. On earlier
Airflow versions, or if the task state store is unavailable at runtime, ``durable=True`` still
recovers a prior run, just via an older mechanism: the operator checks XCom for a cached run id
first, then falls back to scanning the job's run history for a run tagged with this task
instance's identity, and reconnects if it finds one that is still active.

Like the persisted state itself, the stored run id isn't deleted automatically, that only happens
when someone runs ``airflow state-store clean``. If a task's ``retry_delay`` is longer than
``[state_store] default_retention_days`` (30 days by default) and cleanup runs in between, the run
id won't be there for the next retry, and the operator falls back to the XCom/scan mechanism
above rather than reconnecting via task state store. Avoid running cleanup on a schedule shorter
than your longest ``retry_delay``.

To opt out and always start a fresh run on retry, set ``durable=False``:

.. code-block:: python

  glue_job = GlueJobOperator(
      task_id="glue_job",
      job_name="my_glue_job",
      script_location="s3://glue-examples/glue-scripts/sample_aws_glue_job.py",
      durable=False,
  )

Durable execution applies to the synchronous path. When ``deferrable=True`` is set, the Triggerer
already tracks the run across the wait, so deferrable mode takes precedence and ``durable`` has no
effect.

``durable`` supersedes the deprecated ``resume_glue_job_on_retry`` parameter. Passing
``resume_glue_job_on_retry`` still works and maps its value onto ``durable``, but emits an
``AirflowProviderDeprecationWarning`` on Airflow 3.3 and newer.

.. _howto/operator:GlueDataQualityOperator:

Create an AWS Glue Data Quality
===============================

AWS Glue Data Quality allows you to measure and monitor the quality
of your data so that you can make good business decisions.
To create a new AWS Glue Data Quality ruleset or update an existing one you can
use :class:`~airflow.providers.amazon.aws.operators.glue.GlueDataQualityOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue_data_quality.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_data_quality_operator]
    :end-before: [END howto_operator_glue_data_quality_operator]

.. _howto/operator:GlueDataQualityRuleSetEvaluationRunOperator:

Start a AWS Glue Data Quality Evaluation Run
=============================================

To start a AWS Glue Data Quality ruleset evaluation run you can use
:class:`~airflow.providers.amazon.aws.operators.glue.GlueDataQualityRuleSetEvaluationRunOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue_data_quality.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_data_quality_ruleset_evaluation_run_operator]
    :end-before: [END howto_operator_glue_data_quality_ruleset_evaluation_run_operator]

.. _howto/operator:GlueDataQualityRuleRecommendationRunOperator:

Start a AWS Glue Data Quality Recommendation Run
=================================================

To start a AWS Glue Data Quality rule recommendation run you can use
:class:`~airflow.providers.amazon.aws.operators.glue.GlueDataQualityRuleRecommendationRunOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue_data_quality_with_recommendation.py
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

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_glue_crawler]
    :end-before: [END howto_sensor_glue_crawler]

.. _howto/sensor:GlueJobSensor:

Wait on an AWS Glue job state
=============================

To wait on the state of an AWS Glue Job until it reaches a terminal state you can
use :class:`~airflow.providers.amazon.aws.sensors.glue.GlueJobSensor`

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_glue]
    :end-before: [END howto_sensor_glue]

.. _howto/sensor:GlueDataQualityRuleSetEvaluationRunSensor:

Wait on an AWS Glue Data Quality Evaluation Run
================================================

To wait on the state of an AWS Glue Data Quality RuleSet Evaluation Run until it
reaches a terminal state you can use :class:`~airflow.providers.amazon.aws.sensors.glue.GlueDataQualityRuleSetEvaluationRunSensor`

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue_data_quality.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_glue_data_quality_ruleset_evaluation_run]
    :end-before: [END howto_sensor_glue_data_quality_ruleset_evaluation_run]

.. _howto/sensor:GlueDataQualityRuleRecommendationRunSensor:

Wait on an AWS Glue Data Quality Recommendation Run
====================================================

To wait on the state of an AWS Glue Data Quality recommendation run until it
reaches a terminal state you can use :class:`~airflow.providers.amazon.aws.sensors.glue.GlueDataQualityRuleRecommendationRunSensor`

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue_data_quality_with_recommendation.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_glue_data_quality_rule_recommendation_run]
    :end-before: [END howto_sensor_glue_data_quality_rule_recommendation_run]

.. _howto/sensor:GlueCatalogPartitionSensor:

Wait on an AWS Glue Catalog Partition
======================================

To wait for a partition to show up in AWS Glue Catalog until it
reaches a terminal state you can use :class:`~airflow.providers.amazon.aws.sensors.glue_catalog_partition.GlueCatalogPartitionSensor`

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_glue_catalog_partition]
    :end-before: [END howto_sensor_glue_catalog_partition]

Reference
---------

* `AWS boto3 library documentation for Glue <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html>`__
* `Glue IAM Role creation <https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html>`__
* `AWS boto3 library documentation for Glue DataBrew <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/databrew.html>`__
