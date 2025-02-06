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

:py:mod:`airflow.providers.amazon.aws.operators.redshift_cluster`
=================================================================

.. py:module:: airflow.providers.amazon.aws.operators.redshift_cluster


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftCreateClusterOperator
   airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftCreateClusterSnapshotOperator
   airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftDeleteClusterSnapshotOperator
   airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftResumeClusterOperator
   airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftPauseClusterOperator
   airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftDeleteClusterOperator




.. py:class:: RedshiftCreateClusterOperator(*, cluster_identifier, node_type, master_username, master_user_password, cluster_type = 'multi-node', db_name = 'dev', number_of_nodes = 1, cluster_security_groups = None, vpc_security_group_ids = None, cluster_subnet_group_name = None, availability_zone = None, preferred_maintenance_window = None, cluster_parameter_group_name = None, automated_snapshot_retention_period = 1, manual_snapshot_retention_period = None, port = 5439, cluster_version = '1.0', allow_version_upgrade = True, publicly_accessible = True, encrypted = False, hsm_client_certificate_identifier = None, hsm_configuration_identifier = None, elastic_ip = None, tags = None, kms_key_id = None, enhanced_vpc_routing = False, additional_info = None, iam_roles = None, maintenance_track_name = None, snapshot_schedule_identifier = None, availability_zone_relocation = None, aqua_configuration_status = None, default_iam_role_arn = None, aws_conn_id = 'aws_default', wait_for_completion = False, max_attempt = 5, poll_interval = 60, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Creates a new cluster with the specified parameters.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RedshiftCreateClusterOperator`

   :param cluster_identifier:  A unique identifier for the cluster.
   :param node_type: The node type to be provisioned for the cluster.
           Valid Values: ``ds2.xlarge``, ``ds2.8xlarge``, ``dc1.large``,
           ``dc1.8xlarge``, ``dc2.large``, ``dc2.8xlarge``, ``ra3.xlplus``,
           ``ra3.4xlarge``, and ``ra3.16xlarge``.
   :param master_username: The username associated with the admin user account for
       the cluster that is being created.
   :param master_user_password: The password associated with the admin user account for
       the cluster that is being created.
   :param cluster_type: The type of the cluster ``single-node`` or ``multi-node``.
       The default value is ``multi-node``.
   :param db_name: The name of the first database to be created when the cluster is created.
   :param number_of_nodes: The number of compute nodes in the cluster.
       This param require when ``cluster_type`` is ``multi-node``.
   :param cluster_security_groups: A list of security groups to be associated with this cluster.
   :param vpc_security_group_ids: A list of  VPC security groups to be associated with the cluster.
   :param cluster_subnet_group_name: The name of a cluster subnet group to be associated with this cluster.
   :param availability_zone: The EC2 Availability Zone (AZ).
   :param preferred_maintenance_window: The time range (in UTC) during which automated cluster
       maintenance can occur.
   :param cluster_parameter_group_name: The name of the parameter group to be associated with this cluster.
   :param automated_snapshot_retention_period: The number of days that automated snapshots are retained.
       The default value is ``1``.
   :param manual_snapshot_retention_period: The default number of days to retain a manual snapshot.
   :param port: The port number on which the cluster accepts incoming connections.
       The Default value is ``5439``.
   :param cluster_version: The version of a Redshift engine software that you want to deploy on the cluster.
   :param allow_version_upgrade: Whether major version upgrades can be applied during the maintenance window.
       The Default value is ``True``.
   :param publicly_accessible: Whether cluster can be accessed from a public network.
   :param encrypted: Whether data in the cluster is encrypted at rest.
       The default value is ``False``.
   :param hsm_client_certificate_identifier: Name of the HSM client certificate
       the Amazon Redshift cluster uses to retrieve the data.
   :param hsm_configuration_identifier: Name of the HSM configuration
   :param elastic_ip: The Elastic IP (EIP) address for the cluster.
   :param tags: A list of tag instances
   :param kms_key_id: KMS key id of encryption key.
   :param enhanced_vpc_routing: Whether to create the cluster with enhanced VPC routing enabled
       Default value is ``False``.
   :param additional_info: Reserved
   :param iam_roles: A list of IAM roles that can be used by the cluster to access other AWS services.
   :param maintenance_track_name: Name of the maintenance track for the cluster.
   :param snapshot_schedule_identifier: A  unique identifier for the snapshot schedule.
   :param availability_zone_relocation: Enable relocation for a Redshift cluster
       between Availability Zones after the cluster is created.
   :param aqua_configuration_status: The cluster is configured to use AQUA .
   :param default_iam_role_arn: ARN for the IAM role.
   :param aws_conn_id: str = The Airflow connection used for AWS credentials.
       The default connection id is ``aws_default``.
   :param wait_for_completion: Whether wait for the cluster to be in ``available`` state
   :param max_attempt: The maximum number of attempts to be made. Default: 5
   :param poll_interval: The amount of time in seconds to wait between attempts. Default: 60
   :param deferrable: If True, the operator will run in deferrable mode

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_identifier', 'cluster_type', 'node_type', 'number_of_nodes', 'vpc_security_group_ids')



   .. py:attribute:: ui_color
      :value: '#eeaa11'



   .. py:attribute:: ui_fgcolor
      :value: '#ffffff'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: RedshiftCreateClusterSnapshotOperator(*, snapshot_identifier, cluster_identifier, retention_period = -1, tags = None, wait_for_completion = False, poll_interval = 15, max_attempt = 20, aws_conn_id = 'aws_default', deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Creates a manual snapshot of the specified cluster. The cluster must be in the available state.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RedshiftCreateClusterSnapshotOperator`

   :param snapshot_identifier: A unique identifier for the snapshot that you are requesting
   :param cluster_identifier: The cluster identifier for which you want a snapshot
   :param retention_period: The number of days that a manual snapshot is retained.
       If the value is -1, the manual snapshot is retained indefinitely.
   :param tags: A list of tag instances
   :param wait_for_completion: Whether wait for the cluster snapshot to be in ``available`` state
   :param poll_interval: Time (in seconds) to wait between two consecutive calls to check state
   :param max_attempt: The maximum number of attempts to be made to check the state
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       The default connection id is ``aws_default``
   :param deferrable: If True, the operator will run as a deferrable operator.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_identifier', 'snapshot_identifier')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: RedshiftDeleteClusterSnapshotOperator(*, snapshot_identifier, cluster_identifier, wait_for_completion = True, aws_conn_id = 'aws_default', poll_interval = 10, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Deletes the specified manual snapshot.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RedshiftDeleteClusterSnapshotOperator`

   :param snapshot_identifier: A unique identifier for the snapshot that you are requesting
   :param cluster_identifier: The unique identifier of the cluster the snapshot was created from
   :param wait_for_completion: Whether wait for cluster deletion or not
       The default value is ``True``
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       The default connection id is ``aws_default``
   :param poll_interval: Time (in seconds) to wait between two consecutive calls to check snapshot state

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_identifier', 'snapshot_identifier')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: get_status()



.. py:class:: RedshiftResumeClusterOperator(*, cluster_identifier, aws_conn_id = 'aws_default', wait_for_completion = False, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), poll_interval = 10, max_attempts = 10, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Resume a paused AWS Redshift Cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RedshiftResumeClusterOperator`

   :param cluster_identifier:  Unique identifier of the AWS Redshift cluster
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       The default connection id is ``aws_default``
   :param poll_interval: Time (in seconds) to wait between two consecutive calls to check cluster state
   :param max_attempts: The maximum number of attempts to check the state of the cluster.
   :param wait_for_completion: If True, the operator will wait for the cluster to be in the
       `resumed` state. Default is False.
   :param deferrable: If True, the operator will run as a deferrable operator.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_identifier',)



   .. py:attribute:: ui_color
      :value: '#eeaa11'



   .. py:attribute:: ui_fgcolor
      :value: '#ffffff'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: RedshiftPauseClusterOperator(*, cluster_identifier, aws_conn_id = 'aws_default', deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), poll_interval = 10, max_attempts = 15, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Pause an AWS Redshift Cluster if it has status `available`.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RedshiftPauseClusterOperator`

   :param cluster_identifier: id of the AWS Redshift Cluster
   :param aws_conn_id: aws connection to use
   :param deferrable: Run operator in the deferrable mode
   :param poll_interval: Time (in seconds) to wait between two consecutive calls to check cluster state
   :param max_attempts: Maximum number of attempts to poll the cluster

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_identifier',)



   .. py:attribute:: ui_color
      :value: '#eeaa11'



   .. py:attribute:: ui_fgcolor
      :value: '#ffffff'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: RedshiftDeleteClusterOperator(*, cluster_identifier, skip_final_cluster_snapshot = True, final_cluster_snapshot_identifier = None, wait_for_completion = True, aws_conn_id = 'aws_default', poll_interval = 30, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), max_attempts = 30, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Delete an AWS Redshift cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RedshiftDeleteClusterOperator`

   :param cluster_identifier: unique identifier of a cluster
   :param skip_final_cluster_snapshot: determines cluster snapshot creation
   :param final_cluster_snapshot_identifier: name of final cluster snapshot
   :param wait_for_completion: Whether wait for cluster deletion or not
       The default value is ``True``
   :param aws_conn_id: aws connection to use
   :param poll_interval: Time (in seconds) to wait between two consecutive calls to check cluster state
   :param deferrable: Run operator in the deferrable mode.
   :param max_attempts: (Deferrable mode only) The maximum number of attempts to be made

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_identifier',)



   .. py:attribute:: ui_color
      :value: '#eeaa11'



   .. py:attribute:: ui_fgcolor
      :value: '#ffffff'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)
