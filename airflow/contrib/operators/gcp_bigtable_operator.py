# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Iterable
import google.api_core.exceptions

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.contrib.hooks.gcp_bigtable_hook import BigtableHook
from airflow.utils.decorators import apply_defaults
from google.cloud.bigtable_admin_v2 import enums
from google.cloud.bigtable.table import ClusterState


class BigtableValidationMixin:
    """
    Common class for Cloud Bigtable operators for validating required fields.
    """

    REQUIRED_ATTRIBUTES = []  # type: Iterable[str]

    def _validate_inputs(self):
        for attr_name in self.REQUIRED_ATTRIBUTES:
            if not getattr(self, attr_name):
                raise AirflowException('Empty parameter: {}'.format(attr_name))


class BigtableInstanceCreateOperator(BaseOperator, BigtableValidationMixin):
    """
    Creates a new Cloud Bigtable instance.
    If the Cloud Bigtable instance with the given ID exists, the operator does not
    compare its configuration
    and immediately succeeds. No changes are made to the existing instance.

    For more details about instance creation have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/instance.html#google.cloud.bigtable.instance.Instance.create

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigtableInstanceCreateOperator`

    :type instance_id: str
    :param instance_id: The ID of the Cloud Bigtable instance to create.
    :type main_cluster_id: str
    :param main_cluster_id: The ID for main cluster for the new instance.
    :type main_cluster_zone: str
    :param main_cluster_zone: The zone for main cluster
        See https://cloud.google.com/bigtable/docs/locations for more details.
    :type project_id: str
    :param project_id: Optional, the ID of the GCP project.  If set to None or missing,
            the default project_id from the GCP connection is used.
    :type replica_cluster_id: str
    :param replica_cluster_id: (optional) The ID for replica cluster for the new instance.
    :type replica_cluster_zone: str
    :param replica_cluster_zone: (optional)  The zone for replica cluster.
    :type instance_type: enums.IntEnum
    :param instance_type: (optional) The type of the instance.
    :type instance_display_name: str
    :param instance_display_name: (optional) Human-readable name of the instance. Defaults
        to ``instance_id``.
    :type instance_labels: dict
    :param instance_labels: (optional) Dictionary of labels to associate
        with the instance.
    :type cluster_nodes: int
    :param cluster_nodes: (optional) Number of nodes for cluster.
    :type cluster_storage_type: enums.IntEnum
    :param cluster_storage_type: (optional) The type of storage.
    :type timeout: int
    :param timeout: (optional) timeout (in seconds) for instance creation.
                    If None is not specified, Operator will wait indefinitely.
    """

    REQUIRED_ATTRIBUTES = ('instance_id', 'main_cluster_id',
                           'main_cluster_zone')
    template_fields = ['project_id', 'instance_id', 'main_cluster_id',
                       'main_cluster_zone']

    @apply_defaults
    def __init__(self,
                 instance_id,
                 main_cluster_id,
                 main_cluster_zone,
                 project_id=None,
                 replica_cluster_id=None,
                 replica_cluster_zone=None,
                 instance_display_name=None,
                 instance_type=None,
                 instance_labels=None,
                 cluster_nodes=None,
                 cluster_storage_type=None,
                 timeout=None,
                 *args, **kwargs):
        self.project_id = project_id
        self.instance_id = instance_id
        self.main_cluster_id = main_cluster_id
        self.main_cluster_zone = main_cluster_zone
        self.replica_cluster_id = replica_cluster_id
        self.replica_cluster_zone = replica_cluster_zone
        self.instance_display_name = instance_display_name
        self.instance_type = instance_type
        self.instance_labels = instance_labels
        self.cluster_nodes = cluster_nodes
        self.cluster_storage_type = cluster_storage_type
        self.timeout = timeout
        self._validate_inputs()
        self.hook = BigtableHook()
        super().__init__(*args, **kwargs)

    def execute(self, context):
        instance = self.hook.get_instance(project_id=self.project_id,
                                          instance_id=self.instance_id)
        if instance:
            # Based on Instance.__eq__ instance with the same ID and client is
            # considered as equal.
            self.log.info(
                "The instance '%s' already exists in this project. "
                "Consider it as created",
                self.instance_id
            )
            return
        try:
            self.hook.create_instance(
                project_id=self.project_id,
                instance_id=self.instance_id,
                main_cluster_id=self.main_cluster_id,
                main_cluster_zone=self.main_cluster_zone,
                replica_cluster_id=self.replica_cluster_id,
                replica_cluster_zone=self.replica_cluster_zone,
                instance_display_name=self.instance_display_name,
                instance_type=self.instance_type,
                instance_labels=self.instance_labels,
                cluster_nodes=self.cluster_nodes,
                cluster_storage_type=self.cluster_storage_type,
                timeout=self.timeout,
            )
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.log.error('An error occurred. Exiting.')
            raise e


class BigtableInstanceDeleteOperator(BaseOperator, BigtableValidationMixin):
    """
    Deletes the Cloud Bigtable instance, including its clusters and all related tables.

    For more details about deleting instance have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/instance.html#google.cloud.bigtable.instance.Instance.delete

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigtableInstanceDeleteOperator`

    :type instance_id: str
    :param instance_id: The ID of the Cloud Bigtable instance to delete.
    :param project_id: Optional, the ID of the GCP project.  If set to None or missing,
            the default project_id from the GCP connection is used.
    :type project_id: str
    """
    REQUIRED_ATTRIBUTES = ('instance_id',)
    template_fields = ['project_id', 'instance_id']

    @apply_defaults
    def __init__(self,
                 instance_id,
                 project_id=None,
                 *args, **kwargs):
        self.project_id = project_id
        self.instance_id = instance_id
        self._validate_inputs()
        self.hook = BigtableHook()
        super().__init__(*args, **kwargs)

    def execute(self, context):
        try:
            self.hook.delete_instance(project_id=self.project_id,
                                      instance_id=self.instance_id)
        except google.api_core.exceptions.NotFound:
            self.log.info(
                "The instance '%s' does not exist in project '%s'. "
                "Consider it as deleted",
                self.instance_id, self.project_id
            )
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.log.error('An error occurred. Exiting.')
            raise e


class BigtableTableCreateOperator(BaseOperator, BigtableValidationMixin):
    """
    Creates the table in the Cloud Bigtable instance.

    For more details about creating table have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/table.html#google.cloud.bigtable.table.Table.create

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigtableTableCreateOperator`

    :type instance_id: str
    :param instance_id: The ID of the Cloud Bigtable instance that will
        hold the new table.
    :type table_id: str
    :param table_id: The ID of the table to be created.
    :type project_id: str
    :param project_id: Optional, the ID of the GCP project. If set to None or missing,
            the default project_id from the GCP connection is used.
    :type initial_split_keys: list
    :param initial_split_keys: (Optional) list of row keys in bytes that will be used to
        initially split the table into several tablets.
    :type column_families: dict
    :param column_families: (Optional) A map columns to create.
                            The key is the column_id str and the value is a
                            :class:`google.cloud.bigtable.column_family.GarbageCollectionRule`
    """
    REQUIRED_ATTRIBUTES = ('instance_id', 'table_id')
    template_fields = ['project_id', 'instance_id', 'table_id']

    @apply_defaults
    def __init__(self,
                 instance_id,
                 table_id,
                 project_id=None,
                 initial_split_keys=None,
                 column_families=None,
                 *args, **kwargs):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.initial_split_keys = initial_split_keys or list()
        self.column_families = column_families or dict()
        self._validate_inputs()
        self.hook = BigtableHook()
        self.instance = None
        super().__init__(*args, **kwargs)

    def _compare_column_families(self):
        table_column_families = self.hook.get_column_families_for_table(self.instance,
                                                                        self.table_id)
        if set(table_column_families.keys()) != set(self.column_families.keys()):
            self.log.error("Table '%s' has different set of Column Families",
                           self.table_id)
            self.log.error("Expected: %s", self.column_families.keys())
            self.log.error("Actual: %s", table_column_families.keys())
            return False

        for key in table_column_families.keys():
            # There is difference in structure between local Column Families
            # and remote ones
            # Local `self.column_families` is dict with column_id as key
            # and GarbageCollectionRule as value.
            # Remote `table_column_families` is list of ColumnFamily objects.
            # For more information about ColumnFamily please refer to the documentation:
            # https://googleapis.github.io/google-cloud-python/latest/bigtable/column-family.html#google.cloud.bigtable.column_family.ColumnFamily
            if table_column_families[key].gc_rule != self.column_families[key]:
                self.log.error("Column Family '%s' differs for table '%s'.", key,
                               self.table_id)
                return False
        return True

    def execute(self, context):
        self.instance = self.hook.get_instance(project_id=self.project_id,
                                               instance_id=self.instance_id)
        if not self.instance:
            raise AirflowException(
                "Dependency: instance '{}' does not exist in project '{}'.".
                format(self.instance_id, self.project_id))
        try:
            self.hook.create_table(
                instance=self.instance,
                table_id=self.table_id,
                initial_split_keys=self.initial_split_keys,
                column_families=self.column_families
            )
        except google.api_core.exceptions.AlreadyExists:
            if not self._compare_column_families():
                raise AirflowException(
                    "Table '{}' already exists with different Column Families.".
                    format(self.table_id))
            self.log.info("The table '%s' already exists. Consider it as created",
                          self.table_id)


class BigtableTableDeleteOperator(BaseOperator, BigtableValidationMixin):
    """
    Deletes the Cloud Bigtable table.

    For more details about deleting table have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/table.html#google.cloud.bigtable.table.Table.delete

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigtableTableDeleteOperator`

    :type instance_id: str
    :param instance_id: The ID of the Cloud Bigtable instance.
    :type table_id: str
    :param table_id: The ID of the table to be deleted.
    :type project_id: str
    :param project_id: Optional, the ID of the GCP project. If set to None or missing,
            the default project_id from the GCP connection is used.
    :type app_profile_id: str
    :parm app_profile_id: Application profile.
    """
    REQUIRED_ATTRIBUTES = ('instance_id', 'table_id')
    template_fields = ['project_id', 'instance_id', 'table_id']

    @apply_defaults
    def __init__(self,
                 instance_id,
                 table_id,
                 project_id=None,
                 app_profile_id=None,
                 *args, **kwargs):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.app_profile_id = app_profile_id
        self._validate_inputs()
        self.hook = BigtableHook()
        super().__init__(*args, **kwargs)

    def execute(self, context):
        instance = self.hook.get_instance(project_id=self.project_id,
                                          instance_id=self.instance_id)
        if not instance:
            raise AirflowException("Dependency: instance '{}' does not exist.".format(
                self.instance_id))

        try:
            self.hook.delete_table(
                project_id=self.project_id,
                instance_id=self.instance_id,
                table_id=self.table_id,
            )
        except google.api_core.exceptions.NotFound:
            # It's OK if table doesn't exists.
            self.log.info("The table '%s' no longer exists. Consider it as deleted",
                          self.table_id)
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.log.error('An error occurred. Exiting.')
            raise e


class BigtableClusterUpdateOperator(BaseOperator, BigtableValidationMixin):
    """
    Updates a Cloud Bigtable cluster.

    For more details about updating a Cloud Bigtable cluster,
    have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/cluster.html#google.cloud.bigtable.cluster.Cluster.update

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigtableClusterUpdateOperator`

    :type instance_id: str
    :param instance_id: The ID of the Cloud Bigtable instance.
    :type cluster_id: str
    :param cluster_id: The ID of the Cloud Bigtable cluster to update.
    :type nodes: int
    :param nodes: The desired number of nodes for the Cloud Bigtable cluster.
    :type project_id: str
    :param project_id: Optional, the ID of the GCP project.
    """
    REQUIRED_ATTRIBUTES = ('instance_id', 'cluster_id', 'nodes')
    template_fields = ['project_id', 'instance_id', 'cluster_id', 'nodes']

    @apply_defaults
    def __init__(self,
                 instance_id,
                 cluster_id,
                 nodes,
                 project_id=None,
                 *args, **kwargs):
        self.project_id = project_id
        self.instance_id = instance_id
        self.cluster_id = cluster_id
        self.nodes = nodes
        self._validate_inputs()
        self.hook = BigtableHook()
        super().__init__(*args, **kwargs)

    def execute(self, context):
        instance = self.hook.get_instance(project_id=self.project_id,
                                          instance_id=self.instance_id)
        if not instance:
            raise AirflowException("Dependency: instance '{}' does not exist.".format(
                self.instance_id))

        try:
            self.hook.update_cluster(
                instance=instance,
                cluster_id=self.cluster_id,
                nodes=self.nodes
            )
        except google.api_core.exceptions.NotFound:
            raise AirflowException(
                "Dependency: cluster '{}' does not exist for instance '{}'.".
                format(self.cluster_id, self.instance_id))
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.log.error('An error occurred. Exiting.')
            raise e


class BigtableTableWaitForReplicationSensor(BaseSensorOperator, BigtableValidationMixin):
    """
    Sensor that waits for Cloud Bigtable table to be fully replicated to its clusters.
    No exception will be raised if the instance or the table does not exist.

    For more details about cluster states for a table, have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/table.html#google.cloud.bigtable.table.Table.get_cluster_states

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigtableTableWaitForReplicationSensor`

    :type instance_id: str
    :param instance_id: The ID of the Cloud Bigtable instance.
    :type table_id: str
    :param table_id: The ID of the table to check replication status.
    :type project_id: str
    :param project_id: Optional, the ID of the GCP project.
    """
    REQUIRED_ATTRIBUTES = ('instance_id', 'table_id')
    template_fields = ['project_id', 'instance_id', 'table_id']

    @apply_defaults
    def __init__(self,
                 instance_id,
                 table_id,
                 project_id=None,
                 *args, **kwargs):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self._validate_inputs()
        self.hook = BigtableHook()
        super().__init__(*args, **kwargs)

    def poke(self, context):
        instance = self.hook.get_instance(project_id=self.project_id,
                                          instance_id=self.instance_id)
        if not instance:
            self.log.info("Dependency: instance '%s' does not exist.", self.instance_id)
            return False

        try:
            cluster_states = self.hook.get_cluster_states_for_table(instance=instance,
                                                                    table_id=self.table_id)
        except google.api_core.exceptions.NotFound:
            self.log.info(
                "Dependency: table '%s' does not exist in instance '%s'.",
                self.table_id, self.instance_id)
            return False

        ready_state = ClusterState(enums.Table.ClusterState.ReplicationState.READY)

        is_table_replicated = True
        for cluster_id in cluster_states.keys():
            if cluster_states[cluster_id] != ready_state:
                self.log.info("Table '%s' is not yet replicated on cluster '%s'.",
                              self.table_id, cluster_id)
                is_table_replicated = False

        if not is_table_replicated:
            return False

        self.log.info("Table '%s' is replicated.", self.table_id)
        return True
