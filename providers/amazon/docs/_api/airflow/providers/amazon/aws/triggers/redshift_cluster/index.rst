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

:py:mod:`airflow.providers.amazon.aws.triggers.redshift_cluster`
================================================================

.. py:module:: airflow.providers.amazon.aws.triggers.redshift_cluster


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftCreateClusterTrigger
   airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftPauseClusterTrigger
   airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftCreateClusterSnapshotTrigger
   airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftResumeClusterTrigger
   airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftDeleteClusterTrigger




.. py:class:: RedshiftCreateClusterTrigger(cluster_identifier, poll_interval = None, max_attempt = None, aws_conn_id = 'aws_default', waiter_delay = 15, waiter_max_attempts = 999999)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Trigger for RedshiftCreateClusterOperator.

   The trigger will asynchronously poll the boto3 API and wait for the
   Redshift cluster to be in the `available` state.

   :param cluster_identifier:  A unique identifier for the cluster.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: RedshiftPauseClusterTrigger(cluster_identifier, poll_interval = None, max_attempts = None, aws_conn_id = 'aws_default', waiter_delay = 15, waiter_max_attempts = 999999)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Trigger for RedshiftPauseClusterOperator.

   The trigger will asynchronously poll the boto3 API and wait for the
   Redshift cluster to be in the `paused` state.

   :param cluster_identifier:  A unique identifier for the cluster.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: RedshiftCreateClusterSnapshotTrigger(cluster_identifier, poll_interval = None, max_attempts = None, aws_conn_id = 'aws_default', waiter_delay = 15, waiter_max_attempts = 999999)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Trigger for RedshiftCreateClusterSnapshotOperator.

   The trigger will asynchronously poll the boto3 API and wait for the
   Redshift cluster snapshot to be in the `available` state.

   :param cluster_identifier:  A unique identifier for the cluster.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: RedshiftResumeClusterTrigger(cluster_identifier, poll_interval = None, max_attempts = None, aws_conn_id = 'aws_default', waiter_delay = 15, waiter_max_attempts = 999999)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Trigger for RedshiftResumeClusterOperator.

   The trigger will asynchronously poll the boto3 API and wait for the
   Redshift cluster to be in the `available` state.

   :param cluster_identifier:  A unique identifier for the cluster.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: RedshiftDeleteClusterTrigger(cluster_identifier, poll_interval = None, max_attempts = None, aws_conn_id = 'aws_default', waiter_delay = 30, waiter_max_attempts = 30)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Trigger for RedshiftDeleteClusterOperator.

   :param cluster_identifier:  A unique identifier for the cluster.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param waiter_delay: The amount of time in seconds to wait between attempts.

   .. py:method:: hook()

      Override in subclasses to return the right hook.
