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

:py:mod:`airflow.providers.amazon.aws.hooks.redshift_cluster`
=============================================================

.. py:module:: airflow.providers.amazon.aws.hooks.redshift_cluster


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook
   airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftAsyncHook




.. py:class:: RedshiftHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Redshift.

   This is a thin wrapper around
   :external+boto3:py:class:`boto3.client("redshift") <Redshift.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_identifier',)



   .. py:method:: create_cluster(cluster_identifier, node_type, master_username, master_user_password, params)

      Create a new cluster with the specified parameters.

      .. seealso::
          - :external+boto3:py:meth:`Redshift.Client.create_cluster`

      :param cluster_identifier: A unique identifier for the cluster.
      :param node_type: The node type to be provisioned for the cluster.
          Valid Values: ``ds2.xlarge``, ``ds2.8xlarge``, ``dc1.large``,
          ``dc1.8xlarge``, ``dc2.large``, ``dc2.8xlarge``, ``ra3.xlplus``,
          ``ra3.4xlarge``, and ``ra3.16xlarge``.
      :param master_username: The username associated with the admin user account
          for the cluster that is being created.
      :param master_user_password: password associated with the admin user account
          for the cluster that is being created.
      :param params: Remaining AWS Create cluster API params.


   .. py:method:: cluster_status(cluster_identifier)

      Get status of a cluster.

      .. seealso::
          - :external+boto3:py:meth:`Redshift.Client.describe_clusters`

      :param cluster_identifier: unique identifier of a cluster
      :param skip_final_cluster_snapshot: determines cluster snapshot creation
      :param final_cluster_snapshot_identifier: Optional[str]


   .. py:method:: delete_cluster(cluster_identifier, skip_final_cluster_snapshot = True, final_cluster_snapshot_identifier = None)

      Delete a cluster and optionally create a snapshot.

      .. seealso::
          - :external+boto3:py:meth:`Redshift.Client.delete_cluster`

      :param cluster_identifier: unique identifier of a cluster
      :param skip_final_cluster_snapshot: determines cluster snapshot creation
      :param final_cluster_snapshot_identifier: name of final cluster snapshot


   .. py:method:: describe_cluster_snapshots(cluster_identifier)

      List snapshots for a cluster.

      .. seealso::
          - :external+boto3:py:meth:`Redshift.Client.describe_cluster_snapshots`

      :param cluster_identifier: unique identifier of a cluster


   .. py:method:: restore_from_cluster_snapshot(cluster_identifier, snapshot_identifier)

      Restore a cluster from its snapshot.

      .. seealso::
          - :external+boto3:py:meth:`Redshift.Client.restore_from_cluster_snapshot`

      :param cluster_identifier: unique identifier of a cluster
      :param snapshot_identifier: unique identifier for a snapshot of a cluster


   .. py:method:: create_cluster_snapshot(snapshot_identifier, cluster_identifier, retention_period = -1, tags = None)

      Create a snapshot of a cluster.

      .. seealso::
          - :external+boto3:py:meth:`Redshift.Client.create_cluster_snapshot`

      :param snapshot_identifier: unique identifier for a snapshot of a cluster
      :param cluster_identifier: unique identifier of a cluster
      :param retention_period: The number of days that a manual snapshot is retained.
          If the value is -1, the manual snapshot is retained indefinitely.
      :param tags: A list of tag instances


   .. py:method:: get_cluster_snapshot_status(snapshot_identifier)

      Get Redshift cluster snapshot status.

      If cluster snapshot not found, *None* is returned.

      :param snapshot_identifier: A unique identifier for the snapshot that you are requesting



.. py:class:: RedshiftAsyncHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseAsyncHook`

   Interact with AWS Redshift using aiobotocore library.

   .. py:method:: cluster_status(cluster_identifier, delete_operation = False)
      :async:

      Get the cluster status.

      :param cluster_identifier: unique identifier of a cluster
      :param delete_operation: whether the method has been called as part of delete cluster operation


   .. py:method:: pause_cluster(cluster_identifier, poll_interval = 5.0)
      :async:

      Pause the cluster.

      :param cluster_identifier: unique identifier of a cluster
      :param poll_interval: polling period in seconds to check for the status


   .. py:method:: resume_cluster(cluster_identifier, polling_period_seconds = 5.0)
      :async:

      Resume the cluster.

      :param cluster_identifier: unique identifier of a cluster
      :param polling_period_seconds: polling period in seconds to check for the status


   .. py:method:: get_cluster_status(cluster_identifier, expected_state, flag, delete_operation = False)
      :async:

      Check for expected Redshift cluster state.

      :param cluster_identifier: unique identifier of a cluster
      :param expected_state: expected_state example("available", "pausing", "paused"")
      :param flag: asyncio even flag set true if success and if any error
      :param delete_operation: whether the method has been called as part of delete cluster operation
