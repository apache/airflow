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

Alibaba Cloud EMR Serverless Spark Operator
===========================================

Overview
--------

Alibaba Cloud EMR Serverless Spark Operator provides:

 - :class:`~airflow.providers.alibaba.cloud.operators.emr.EmrServerlessSparkStartJobRunOperator`

Develop Spark batch applications
-------------------------------------------

Purpose
"""""""

This example dag uses ``EmrServerlessSparkStartJobRunOperator`` to submit spark jobs to EMR Serverless Spark Service.

Defining tasks
""""""""""""""

In the following code we submit Spark Pi application to EMR Serverless Spark Service.

.. exampleinclude:: /../../tests/system/providers/alibaba/example_emr_serverless_spark.py
    :language: python
    :start-after: [START howto_operator_emr_serverless_spark]
    :end-before: [END howto_operator_emr_serverless_spark]
