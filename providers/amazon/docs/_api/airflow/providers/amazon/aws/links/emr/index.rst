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

:py:mod:`airflow.providers.amazon.aws.links.emr`
================================================

.. py:module:: airflow.providers.amazon.aws.links.emr


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.links.emr.EmrClusterLink
   airflow.providers.amazon.aws.links.emr.EmrLogsLink



Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.links.emr.get_log_uri



.. py:class:: EmrClusterLink


   Bases: :py:obj:`airflow.providers.amazon.aws.links.base_aws.BaseAwsLink`

   Helper class for constructing AWS EMR Cluster Link.

   .. py:attribute:: name
      :value: 'EMR Cluster'



   .. py:attribute:: key
      :value: 'emr_cluster'



   .. py:attribute:: format_str




.. py:class:: EmrLogsLink


   Bases: :py:obj:`airflow.providers.amazon.aws.links.base_aws.BaseAwsLink`

   Helper class for constructing AWS EMR Logs Link.

   .. py:attribute:: name
      :value: 'EMR Cluster Logs'



   .. py:attribute:: key
      :value: 'emr_logs'



   .. py:attribute:: format_str



   .. py:method:: format_link(**kwargs)

      Format AWS Service Link.

      Some AWS Service Link should require additional escaping
      in this case this method should be overridden.



.. py:function:: get_log_uri(*, cluster = None, emr_client = None, job_flow_id = None)

   Retrieve the S3 URI to the EMR Job logs.

   Requires either the output of a describe_cluster call or both an EMR Client and a job_flow_id..
