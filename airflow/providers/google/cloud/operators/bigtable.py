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
"""This module contains Google Cloud Bigtable operators."""
from __future__ import annotations

import enum
from typing import TYPE_CHECKING, Iterable, Sequence

import google.api_core.exceptions
from google.cloud.bigtable import enums
from google.cloud.bigtable.column_family import GarbageCollectionRule

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.bigtable import BigtableHook
from airflow.providers.google.cloud.links.bigtable import (
    BigtableClusterLink,
    BigtableInstanceLink,
    BigtableTablesLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class BigtableValidationMixin:
    """Common class for Cloud Bigtable operators for validating required fields."""

    REQUIRED_ATTRIBUTES = []  # type: Iterable[str]

    def _validate_inputs(self):
        for attr_name in self.REQUIRED_ATTRIBUTES:
            if not getattr(self, attr_name):
                raise AirflowException(f"Empty parameter: {attr_name}")


class BigtableCreateInstanceOperator(GoogleCloudBaseOperator, BigtableValidationMixin):
    """
    Creates a new Cloud Bigtable instance.
    If the Cloud Bigtable instance with the given ID exists, the operator does not
    compare its configuration
    and immediately succeeds. No changes are made to the existing instance.

    For more details about instance creation have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/instance.html#google.cloud.bigtable.instance.Instance.create

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigtableCreateInstanceOperator`

    :param instance_id: The ID of the Cloud Bigtable instance to create.
    :param main_cluster_id: The ID for main cluster for the new instance.
    :param main_cluster_zone: The zone for main cluster
        See https://cloud.google.com/bigtable/docs/locations for more details.
    :param project_id: Optional, the ID of the Google Cloud project.  If set to None or missing,
            the default project_id from the Google Cloud connection is used.
    :param replica_clusters: (optional) A list of replica clusters for the new
        instance. Each cluster dictionary contains an id and a zone.
        Example: [{"id": "replica-1", "zone": "us-west1-a"}]
    :param instance_type: (optional) The type of the instance.
    :param instance_display_name: (optional) Human-readable name of the instance. Defaults
        to ``instance_id``.
    :param instance_labels: (optional) Dictionary of labels to associate
        with the instance.
    :param cluster_nodes: (optional) Number of nodes for cluster.
    :param cluster_storage_type: (optional) The type of storage.
    :param timeout: (optional) timeout (in seconds) for instance creation.
                    If None is not specified, Operator will wait indefinitely.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    REQUIRED_ATTRIBUTES: Iterable[str] = ("instance_id", "main_cluster_id", "main_cluster_zone")
    template_fields: Sequence[str] = (
        "project_id",
        "instance_id",
        "main_cluster_id",
        "main_cluster_zone",
        "impersonation_chain",
    )
    operator_extra_links = (BigtableInstanceLink(),)

    def __init__(
        self,
        *,
        instance_id: str,
        main_cluster_id: str,
        main_cluster_zone: str,
        project_id: str | None = None,
        replica_clusters: list[dict[str, str]] | None = None,
        instance_display_name: str | None = None,
        instance_type: enums.Instance.Type | None = None,
        instance_labels: dict | None = None,
        cluster_nodes: int | None = None,
        cluster_storage_type: enums.StorageType | None = None,
        timeout: float | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.instance_id = instance_id
        self.main_cluster_id = main_cluster_id
        self.main_cluster_zone = main_cluster_zone
        self.replica_clusters = replica_clusters
        self.instance_display_name = instance_display_name
        self.instance_type = instance_type
        self.instance_labels = instance_labels
        self.cluster_nodes = cluster_nodes
        self.cluster_storage_type = cluster_storage_type
        self.timeout = timeout
        self._validate_inputs()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        hook = BigtableHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        instance = hook.get_instance(project_id=self.project_id, instance_id=self.instance_id)
        if instance:
            # Based on Instance.__eq__ instance with the same ID and client is
            # considered as equal.
            self.log.info(
                "The instance '%s' already exists in this project. Consider it as created",
                self.instance_id,
            )
            BigtableInstanceLink.persist(context=context, task_instance=self)
            return
        try:
            hook.create_instance(
                project_id=self.project_id,
                instance_id=self.instance_id,
                main_cluster_id=self.main_cluster_id,
                main_cluster_zone=self.main_cluster_zone,
                replica_clusters=self.replica_clusters,
                instance_display_name=self.instance_display_name,
                instance_type=self.instance_type,
                instance_labels=self.instance_labels,
                cluster_nodes=self.cluster_nodes,
                cluster_storage_type=self.cluster_storage_type,
                timeout=self.timeout,
            )
            BigtableInstanceLink.persist(context=context, task_instance=self)
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.log.error("An error occurred. Exiting.")
            raise e


class BigtableUpdateInstanceOperator(GoogleCloudBaseOperator, BigtableValidationMixin):
    """
    Updates an existing Cloud Bigtable instance.

    For more details about instance creation have a look at the reference:
    https://googleapis.dev/python/bigtable/latest/instance.html#google.cloud.bigtable.instance.Instance.update

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigtableUpdateInstanceOperator`

    :param instance_id: The ID of the Cloud Bigtable instance to update.
    :param project_id: Optional, the ID of the Google Cloud project. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
    :param instance_display_name: (optional) Human-readable name of the instance.
    :param instance_type: (optional) The type of the instance.
    :param instance_labels: (optional) Dictionary of labels to associate
        with the instance.
    :param timeout: (optional) timeout (in seconds) for instance update.
                    If None is not specified, Operator will wait indefinitely.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    REQUIRED_ATTRIBUTES: Iterable[str] = ["instance_id"]
    template_fields: Sequence[str] = (
        "project_id",
        "instance_id",
        "impersonation_chain",
    )
    operator_extra_links = (BigtableInstanceLink(),)

    def __init__(
        self,
        *,
        instance_id: str,
        project_id: str | None = None,
        instance_display_name: str | None = None,
        instance_type: enums.Instance.Type | enum.IntEnum | None = None,
        instance_labels: dict | None = None,
        timeout: float | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.instance_id = instance_id
        self.instance_display_name = instance_display_name
        self.instance_type = instance_type
        self.instance_labels = instance_labels
        self.timeout = timeout
        self._validate_inputs()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        hook = BigtableHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        instance = hook.get_instance(project_id=self.project_id, instance_id=self.instance_id)
        if not instance:
            raise AirflowException(f"Dependency: instance '{self.instance_id}' does not exist.")

        try:
            hook.update_instance(
                project_id=self.project_id,
                instance_id=self.instance_id,
                instance_display_name=self.instance_display_name,
                instance_type=self.instance_type,
                instance_labels=self.instance_labels,
                timeout=self.timeout,
            )
            BigtableInstanceLink.persist(context=context, task_instance=self)
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.log.error("An error occurred. Exiting.")
            raise e


class BigtableDeleteInstanceOperator(GoogleCloudBaseOperator, BigtableValidationMixin):
    """
    Deletes the Cloud Bigtable instance, including its clusters and all related tables.

    For more details about deleting instance have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/instance.html#google.cloud.bigtable.instance.Instance.delete

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigtableDeleteInstanceOperator`

    :param instance_id: The ID of the Cloud Bigtable instance to delete.
    :param project_id: Optional, the ID of the Google Cloud project.  If set to None or missing,
            the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    REQUIRED_ATTRIBUTES = ("instance_id",)  # type: Iterable[str]
    template_fields: Sequence[str] = (
        "project_id",
        "instance_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        instance_id: str,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.instance_id = instance_id
        self._validate_inputs()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        hook = BigtableHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            hook.delete_instance(project_id=self.project_id, instance_id=self.instance_id)
        except google.api_core.exceptions.NotFound:
            self.log.info(
                "The instance '%s' does not exist in project '%s'. Consider it as deleted",
                self.instance_id,
                self.project_id,
            )
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.log.error("An error occurred. Exiting.")
            raise e


class BigtableCreateTableOperator(GoogleCloudBaseOperator, BigtableValidationMixin):
    """
    Creates the table in the Cloud Bigtable instance.

    For more details about creating table have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/table.html#google.cloud.bigtable.table.Table.create

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigtableCreateTableOperator`

    :param instance_id: The ID of the Cloud Bigtable instance that will
        hold the new table.
    :param table_id: The ID of the table to be created.
    :param project_id: Optional, the ID of the Google Cloud project. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
    :param initial_split_keys: (Optional) list of row keys in bytes that will be used to
        initially split the table into several tablets.
    :param column_families: (Optional) A map columns to create.
                            The key is the column_id str and the value is a
                            :class:`google.cloud.bigtable.column_family.GarbageCollectionRule`
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    REQUIRED_ATTRIBUTES = ("instance_id", "table_id")  # type: Iterable[str]
    template_fields: Sequence[str] = (
        "project_id",
        "instance_id",
        "table_id",
        "impersonation_chain",
    )
    operator_extra_links = (BigtableTablesLink(),)

    def __init__(
        self,
        *,
        instance_id: str,
        table_id: str,
        project_id: str | None = None,
        initial_split_keys: list | None = None,
        column_families: dict[str, GarbageCollectionRule] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.initial_split_keys = initial_split_keys or []
        self.column_families = column_families or {}
        self._validate_inputs()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def _compare_column_families(self, hook, instance) -> bool:
        table_column_families = hook.get_column_families_for_table(instance, self.table_id)
        if set(table_column_families.keys()) != set(self.column_families.keys()):
            self.log.error("Table '%s' has different set of Column Families", self.table_id)
            self.log.error("Expected: %s", self.column_families.keys())
            self.log.error("Actual: %s", table_column_families.keys())
            return False

        for key in table_column_families:
            # There is difference in structure between local Column Families
            # and remote ones
            # Local `self.column_families` is dict with column_id as key
            # and GarbageCollectionRule as value.
            # Remote `table_column_families` is list of ColumnFamily objects.
            # For more information about ColumnFamily please refer to the documentation:
            # https://googleapis.github.io/google-cloud-python/latest/bigtable/column-family.html#google.cloud.bigtable.column_family.ColumnFamily
            if table_column_families[key].gc_rule != self.column_families[key]:
                self.log.error("Column Family '%s' differs for table '%s'.", key, self.table_id)
                return False
        return True

    def execute(self, context: Context) -> None:
        hook = BigtableHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        instance = hook.get_instance(project_id=self.project_id, instance_id=self.instance_id)
        if not instance:
            raise AirflowException(
                f"Dependency: instance '{self.instance_id}' does not exist in project '{self.project_id}'."
            )
        try:
            hook.create_table(
                instance=instance,
                table_id=self.table_id,
                initial_split_keys=self.initial_split_keys,
                column_families=self.column_families,
            )
            BigtableTablesLink.persist(context=context, task_instance=self)
        except google.api_core.exceptions.AlreadyExists:
            if not self._compare_column_families(hook, instance):
                raise AirflowException(
                    f"Table '{self.table_id}' already exists with different Column Families."
                )
            self.log.info("The table '%s' already exists. Consider it as created", self.table_id)


class BigtableDeleteTableOperator(GoogleCloudBaseOperator, BigtableValidationMixin):
    """
    Deletes the Cloud Bigtable table.

    For more details about deleting table have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/table.html#google.cloud.bigtable.table.Table.delete

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigtableDeleteTableOperator`

    :param instance_id: The ID of the Cloud Bigtable instance.
    :param table_id: The ID of the table to be deleted.
    :param project_id: Optional, the ID of the Google Cloud project. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
    :param app_profile_id: Application profile.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    REQUIRED_ATTRIBUTES = ("instance_id", "table_id")  # type: Iterable[str]
    template_fields: Sequence[str] = (
        "project_id",
        "instance_id",
        "table_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        instance_id: str,
        table_id: str,
        project_id: str | None = None,
        app_profile_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.app_profile_id = app_profile_id
        self._validate_inputs()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        hook = BigtableHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        instance = hook.get_instance(project_id=self.project_id, instance_id=self.instance_id)
        if not instance:
            raise AirflowException(f"Dependency: instance '{self.instance_id}' does not exist.")

        try:
            hook.delete_table(
                project_id=self.project_id,
                instance_id=self.instance_id,
                table_id=self.table_id,
            )
        except google.api_core.exceptions.NotFound:
            # It's OK if table doesn't exists.
            self.log.info("The table '%s' no longer exists. Consider it as deleted", self.table_id)
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.log.error("An error occurred. Exiting.")
            raise e


class BigtableUpdateClusterOperator(GoogleCloudBaseOperator, BigtableValidationMixin):
    """
    Updates a Cloud Bigtable cluster.

    For more details about updating a Cloud Bigtable cluster,
    have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/cluster.html#google.cloud.bigtable.cluster.Cluster.update

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigtableUpdateClusterOperator`

    :param instance_id: The ID of the Cloud Bigtable instance.
    :param cluster_id: The ID of the Cloud Bigtable cluster to update.
    :param nodes: The desired number of nodes for the Cloud Bigtable cluster.
    :param project_id: Optional, the ID of the Google Cloud project.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    REQUIRED_ATTRIBUTES = ("instance_id", "cluster_id", "nodes")  # type: Iterable[str]
    template_fields: Sequence[str] = (
        "project_id",
        "instance_id",
        "cluster_id",
        "nodes",
        "impersonation_chain",
    )
    operator_extra_links = (BigtableClusterLink(),)

    def __init__(
        self,
        *,
        instance_id: str,
        cluster_id: str,
        nodes: int,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.instance_id = instance_id
        self.cluster_id = cluster_id
        self.nodes = nodes
        self._validate_inputs()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        hook = BigtableHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        instance = hook.get_instance(project_id=self.project_id, instance_id=self.instance_id)
        if not instance:
            raise AirflowException(f"Dependency: instance '{self.instance_id}' does not exist.")

        try:
            hook.update_cluster(instance=instance, cluster_id=self.cluster_id, nodes=self.nodes)
            BigtableClusterLink.persist(context=context, task_instance=self)
        except google.api_core.exceptions.NotFound:
            raise AirflowException(
                f"Dependency: cluster '{self.cluster_id}' does not exist for instance '{self.instance_id}'."
            )
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.log.error("An error occurred. Exiting.")
            raise e
