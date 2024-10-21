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

Alibaba Cloud AnalyticDB Spark Operators
========================================

Overview
--------

Airflow to Alibaba Cloud AnalyticDB Spark integration provides several operators to develop spark batch and sql applications.

 - :class:`~airflow.providers.alibaba.cloud.operators.analyticdb_spark.AnalyticDBSparkBatchOperator`
 - :class:`~airflow.providers.alibaba.cloud.operators.analyticdb_spark.AnalyticDBSparkSQLOperator`

Develop Spark batch applications
-------------------------------------------

Purpose
"""""""

This example dag uses ``AnalyticDBSparkBatchOperator`` to submit Spark Pi and Spark Logistic regression applications.

Defining tasks
""""""""""""""

In the following code we submit Spark Pi and Spark Logistic regression applications.

.. exampleinclude:: /../../providers/tests/system/alibaba/example_adb_spark_batch.py
    :language: python
    :start-after: [START howto_operator_adb_spark_batch]
    :end-before: [END howto_operator_adb_spark_batch]
