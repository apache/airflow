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

:py:mod:`airflow.providers.amazon.aws.utils.sagemaker`
======================================================

.. py:module:: airflow.providers.amazon.aws.utils.sagemaker


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.utils.sagemaker.ApprovalStatus




.. py:class:: ApprovalStatus


   Bases: :py:obj:`enum.Enum`

   Approval statuses for a Sagemaker Model Package.

   .. py:attribute:: APPROVED
      :value: 'Approved'



   .. py:attribute:: REJECTED
      :value: 'Rejected'



   .. py:attribute:: PENDING_MANUAL_APPROVAL
      :value: 'PendingManualApproval'
