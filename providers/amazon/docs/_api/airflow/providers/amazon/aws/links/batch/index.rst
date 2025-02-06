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

:py:mod:`airflow.providers.amazon.aws.links.batch`
==================================================

.. py:module:: airflow.providers.amazon.aws.links.batch


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.links.batch.BatchJobDefinitionLink
   airflow.providers.amazon.aws.links.batch.BatchJobDetailsLink
   airflow.providers.amazon.aws.links.batch.BatchJobQueueLink




.. py:class:: BatchJobDefinitionLink


   Bases: :py:obj:`airflow.providers.amazon.aws.links.base_aws.BaseAwsLink`

   Helper class for constructing AWS Batch Job Definition Link.

   .. py:attribute:: name
      :value: 'Batch Job Definition'



   .. py:attribute:: key
      :value: 'batch_job_definition'



   .. py:attribute:: format_str




.. py:class:: BatchJobDetailsLink


   Bases: :py:obj:`airflow.providers.amazon.aws.links.base_aws.BaseAwsLink`

   Helper class for constructing AWS Batch Job Details Link.

   .. py:attribute:: name
      :value: 'Batch Job Details'



   .. py:attribute:: key
      :value: 'batch_job_details'



   .. py:attribute:: format_str




.. py:class:: BatchJobQueueLink


   Bases: :py:obj:`airflow.providers.amazon.aws.links.base_aws.BaseAwsLink`

   Helper class for constructing AWS Batch Job Queue Link.

   .. py:attribute:: name
      :value: 'Batch Job Queue'



   .. py:attribute:: key
      :value: 'batch_job_queue'



   .. py:attribute:: format_str
