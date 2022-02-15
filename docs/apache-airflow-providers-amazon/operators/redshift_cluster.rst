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

Redshift cluster management operators
=====================================

.. contents::
  :depth: 1
  :local:

.. _howto/operator:RedshiftResumeClusterOperator:

Resume a Redshift Cluster
"""""""""""""""""""""""""

To resume a 'paused' AWS Redshift Cluster you can use
:class:`RedshiftResumeClusterOperator <airflow.providers.amazon.aws.operators.redshift_cluster>`

This Operator leverages the AWS CLI
`resume-cluster <https://docs.aws.amazon.com/cli/latest/reference/redshift/resume-cluster.html>`__ API

.. _howto/operator:RedshiftPauseClusterOperator:

Pause a Redshift Cluster
""""""""""""""""""""""""

To pause an 'available' AWS Redshift Cluster you can use
:class:`RedshiftPauseClusterOperator <airflow.providers.amazon.aws.operators.redshift_cluster>`
This Operator leverages the AWS CLI
`pause-cluster <https://docs.aws.amazon.com/cli/latest/reference/redshift/pause-cluster.html>`__ API
