:mod:`airflow.providers.amazon.aws.hooks.redshift`
==================================================

.. py:module:: airflow.providers.amazon.aws.hooks.redshift

.. autoapi-nested-parse::

   Interact with AWS Redshift, using the boto3 library.



Module Contents
---------------

.. py:class:: RedshiftHook(*args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS Redshift, using the boto3 library

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   
   .. method:: cluster_status(self, cluster_identifier: str)

      Return status of a cluster

      :param cluster_identifier: unique identifier of a cluster
      :type cluster_identifier: str



   
   .. method:: delete_cluster(self, cluster_identifier: str, skip_final_cluster_snapshot: bool = True, final_cluster_snapshot_identifier: Optional[str] = None)

      Delete a cluster and optionally create a snapshot

      :param cluster_identifier: unique identifier of a cluster
      :type cluster_identifier: str
      :param skip_final_cluster_snapshot: determines cluster snapshot creation
      :type skip_final_cluster_snapshot: bool
      :param final_cluster_snapshot_identifier: name of final cluster snapshot
      :type final_cluster_snapshot_identifier: str



   
   .. method:: describe_cluster_snapshots(self, cluster_identifier: str)

      Gets a list of snapshots for a cluster

      :param cluster_identifier: unique identifier of a cluster
      :type cluster_identifier: str



   
   .. method:: restore_from_cluster_snapshot(self, cluster_identifier: str, snapshot_identifier: str)

      Restores a cluster from its snapshot

      :param cluster_identifier: unique identifier of a cluster
      :type cluster_identifier: str
      :param snapshot_identifier: unique identifier for a snapshot of a cluster
      :type snapshot_identifier: str



   
   .. method:: create_cluster_snapshot(self, snapshot_identifier: str, cluster_identifier: str)

      Creates a snapshot of a cluster

      :param snapshot_identifier: unique identifier for a snapshot of a cluster
      :type snapshot_identifier: str
      :param cluster_identifier: unique identifier of a cluster
      :type cluster_identifier: str




