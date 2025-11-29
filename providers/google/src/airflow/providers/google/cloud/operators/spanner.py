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
"""This module contains Google Spanner operators."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.spanner import SpannerHook
from airflow.providers.google.cloud.links.spanner import SpannerDatabaseLink, SpannerInstanceLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context
    from airflow.providers.openlineage.extractors import OperatorLineage


class SpannerDeployInstanceOperator(GoogleCloudBaseOperator):
    """
    Create or update a Cloud Spanner instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SpannerDeployInstanceOperator`

    :param instance_id: Cloud Spanner instance ID.
    :param configuration_name:  The name of the Cloud Spanner instance configuration
      defining how the instance will be created. Required for
      instances that do not yet exist.
    :param node_count: (Optional) The number of nodes allocated to the Cloud Spanner
      instance.
    :param display_name: (Optional) The display name for the Cloud Spanner  instance in
      the Google Cloud Console. (Must be between 4 and 30 characters.) If this value is not set
      in the constructor, the name is the same as the instance ID.
    :param project_id: Optional, the ID of the project which owns the Cloud Spanner
        Database.  If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_spanner_deploy_template_fields]
    template_fields: Sequence[str] = (
        "project_id",
        "instance_id",
        "configuration_name",
        "display_name",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END gcp_spanner_deploy_template_fields]
    operator_extra_links = (SpannerInstanceLink(),)

    def __init__(
        self,
        *,
        instance_id: str,
        configuration_name: str,
        node_count: int,
        display_name: str,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.instance_id = instance_id
        self.project_id = project_id
        self.configuration_name = configuration_name
        self.node_count = node_count
        self.display_name = display_name
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def _validate_inputs(self) -> None:
        if self.project_id == "":
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty or None")

    def execute(self, context: Context) -> None:
        hook = SpannerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        if not hook.get_instance(project_id=self.project_id, instance_id=self.instance_id):
            self.log.info("Creating Cloud Spanner instance '%s'", self.instance_id)
            func = hook.create_instance
        else:
            self.log.info("Updating Cloud Spanner instance '%s'", self.instance_id)
            func = hook.update_instance
        func(
            project_id=self.project_id,
            instance_id=self.instance_id,
            configuration_name=self.configuration_name,
            node_count=self.node_count,
            display_name=self.display_name,
        )
        SpannerInstanceLink.persist(
            context=context,
            instance_id=self.instance_id,
            project_id=self.project_id or hook.project_id,
        )


class SpannerDeleteInstanceOperator(GoogleCloudBaseOperator):
    """
    Delete a Cloud Spanner instance; if an instance does not exist, no action is taken and the task succeeds.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SpannerDeleteInstanceOperator`

    :param instance_id: The Cloud Spanner instance ID.
    :param project_id: Optional, the ID of the project that owns the Cloud Spanner
        Database.  If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_spanner_delete_template_fields]
    template_fields: Sequence[str] = (
        "project_id",
        "instance_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END gcp_spanner_delete_template_fields]

    def __init__(
        self,
        *,
        instance_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.instance_id = instance_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def _validate_inputs(self) -> None:
        if self.project_id == "":
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty or None")

    def execute(self, context: Context) -> bool | None:
        hook = SpannerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        if hook.get_instance(project_id=self.project_id, instance_id=self.instance_id):
            return hook.delete_instance(project_id=self.project_id, instance_id=self.instance_id)
        self.log.info(
            "Instance '%s' does not exist in project '%s'. Aborting delete.",
            self.instance_id,
            self.project_id,
        )
        return True


class SpannerQueryDatabaseInstanceOperator(GoogleCloudBaseOperator):
    """
    Executes an arbitrary DML query (INSERT, UPDATE, DELETE).

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SpannerQueryDatabaseInstanceOperator`

    :param instance_id: The Cloud Spanner instance ID.
    :param database_id: The Cloud Spanner database ID.
    :param query: The query or list of queries to be executed. Can be a path to a SQL
       file.
    :param project_id: Optional, the ID of the project that owns the Cloud Spanner
        Database.  If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_spanner_query_template_fields]
    template_fields: Sequence[str] = (
        "project_id",
        "instance_id",
        "database_id",
        "query",
        "gcp_conn_id",
        "impersonation_chain",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"query": "sql"}
    # [END gcp_spanner_query_template_fields]
    operator_extra_links = (SpannerDatabaseLink(),)

    def __init__(
        self,
        *,
        instance_id: str,
        database_id: str,
        query: str | list[str],
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.instance_id = instance_id
        self.project_id = project_id
        self.database_id = database_id
        self.query = query
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    @cached_property
    def hook(self) -> SpannerHook:
        return SpannerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def _validate_inputs(self) -> None:
        if self.project_id == "":
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty or None")
        if not self.database_id:
            raise AirflowException("The required parameter 'database_id' is empty or None")
        if not self.query:
            raise AirflowException("The required parameter 'query' is empty")

    def execute(self, context: Context):
        if isinstance(self.query, str):
            queries = [x.strip() for x in self.query.split(";")]
            self.sanitize_queries(queries)
        else:
            queries = self.query
        self.log.info(
            "Executing DML query(-ies) on projects/%s/instances/%s/databases/%s",
            self.project_id,
            self.instance_id,
            self.database_id,
        )
        self.log.info("Executing queries: %s", queries)
        result_rows_count_per_query = self.hook.execute_dml(
            project_id=self.project_id,
            instance_id=self.instance_id,
            database_id=self.database_id,
            queries=queries,
        )
        SpannerDatabaseLink.persist(
            context=context,
            instance_id=self.instance_id,
            database_id=self.database_id,
            project_id=self.project_id or self.hook.project_id,
        )
        return result_rows_count_per_query

    @staticmethod
    def sanitize_queries(queries: list[str]) -> None:
        """
        Drop empty query in queries.

        :param queries: queries
        """
        if queries and queries[-1] == "":
            queries.pop()

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage | None:
        """Build a generic OpenLineage facet, aligned with SQL-based operators."""
        from airflow.providers.common.compat.openlineage.utils.sql import get_openlineage_facets_with_sql

        return get_openlineage_facets_with_sql(
            hook=self.hook,
            sql=self.query,
            conn_id=self.gcp_conn_id,
            database=self.database_id,
        )


class SpannerDeployDatabaseInstanceOperator(GoogleCloudBaseOperator):
    """
    Creates a new Cloud Spanner database; if database exists, the operator does nothing.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SpannerDeployDatabaseInstanceOperator`

    :param instance_id: The Cloud Spanner instance ID.
    :param database_id: The Cloud Spanner database ID.
    :param ddl_statements: The string list containing DDL for the new database.
    :param project_id: Optional, the ID of the project that owns the Cloud Spanner
        Database.  If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_spanner_database_deploy_template_fields]
    template_fields: Sequence[str] = (
        "project_id",
        "instance_id",
        "database_id",
        "ddl_statements",
        "gcp_conn_id",
        "impersonation_chain",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"ddl_statements": "sql"}
    # [END gcp_spanner_database_deploy_template_fields]
    operator_extra_links = (SpannerDatabaseLink(),)

    def __init__(
        self,
        *,
        instance_id: str,
        database_id: str,
        ddl_statements: list[str],
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.instance_id = instance_id
        self.project_id = project_id
        self.database_id = database_id
        self.ddl_statements = ddl_statements
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def _validate_inputs(self) -> None:
        if self.project_id == "":
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty or None")
        if not self.database_id:
            raise AirflowException("The required parameter 'database_id' is empty or None")

    def execute(self, context: Context) -> bool | None:
        hook = SpannerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        SpannerDatabaseLink.persist(
            context=context,
            instance_id=self.instance_id,
            database_id=self.database_id,
            project_id=self.project_id or hook.project_id,
        )
        if not hook.get_database(
            project_id=self.project_id, instance_id=self.instance_id, database_id=self.database_id
        ):
            self.log.info(
                "Creating Cloud Spanner database '%s' in project '%s' and instance '%s'",
                self.database_id,
                self.project_id,
                self.instance_id,
            )
            return hook.create_database(
                project_id=self.project_id,
                instance_id=self.instance_id,
                database_id=self.database_id,
                ddl_statements=self.ddl_statements,
            )
        self.log.info(
            "The database '%s' in project '%s' and instance '%s' already exists. Nothing to do. Exiting.",
            self.database_id,
            self.project_id,
            self.instance_id,
        )
        return True


class SpannerUpdateDatabaseInstanceOperator(GoogleCloudBaseOperator):
    """
    Updates a Cloud Spanner database with the specified DDL statement.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SpannerUpdateDatabaseInstanceOperator`

    :param instance_id: The Cloud Spanner instance ID.
    :param database_id: The Cloud Spanner database ID.
    :param ddl_statements: The string list containing DDL to apply to the database.
    :param project_id: Optional, the ID of the project that owns the Cloud Spanner
        Database.  If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param operation_id: (Optional) Unique per database operation id that can
           be specified to implement idempotency check.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_spanner_database_update_template_fields]
    template_fields: Sequence[str] = (
        "project_id",
        "instance_id",
        "database_id",
        "ddl_statements",
        "gcp_conn_id",
        "impersonation_chain",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"ddl_statements": "sql"}
    # [END gcp_spanner_database_update_template_fields]
    operator_extra_links = (SpannerDatabaseLink(),)

    def __init__(
        self,
        *,
        instance_id: str,
        database_id: str,
        ddl_statements: list[str],
        project_id: str = PROVIDE_PROJECT_ID,
        operation_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.instance_id = instance_id
        self.project_id = project_id
        self.database_id = database_id
        self.ddl_statements = ddl_statements
        self.operation_id = operation_id
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def _validate_inputs(self) -> None:
        if self.project_id == "":
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty or None")
        if not self.database_id:
            raise AirflowException("The required parameter 'database_id' is empty or None")
        if not self.ddl_statements:
            raise AirflowException("The required parameter 'ddl_statements' is empty or None")

    def execute(self, context: Context) -> None:
        hook = SpannerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        if not hook.get_database(
            project_id=self.project_id, instance_id=self.instance_id, database_id=self.database_id
        ):
            raise AirflowException(
                f"The Cloud Spanner database '{self.database_id}' in project '{self.project_id}' "
                f"and instance '{self.instance_id}' is missing. "
                f"Create the database first before you can update it."
            )
        SpannerDatabaseLink.persist(
            context=context,
            instance_id=self.instance_id,
            database_id=self.database_id,
            project_id=self.project_id or hook.project_id,
        )
        return hook.update_database(
            project_id=self.project_id,
            instance_id=self.instance_id,
            database_id=self.database_id,
            ddl_statements=self.ddl_statements,
            operation_id=self.operation_id,
        )


class SpannerDeleteDatabaseInstanceOperator(GoogleCloudBaseOperator):
    """
    Deletes a Cloud Spanner database.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SpannerDeleteDatabaseInstanceOperator`

    :param instance_id: Cloud Spanner instance ID.
    :param database_id: Cloud Spanner database ID.
    :param project_id: Optional, the ID of the project that owns the Cloud Spanner
        Database.  If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_spanner_database_delete_template_fields]
    template_fields: Sequence[str] = (
        "project_id",
        "instance_id",
        "database_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END gcp_spanner_database_delete_template_fields]

    def __init__(
        self,
        *,
        instance_id: str,
        database_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.instance_id = instance_id
        self.project_id = project_id
        self.database_id = database_id
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def _validate_inputs(self) -> None:
        if self.project_id == "":
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty or None")
        if not self.database_id:
            raise AirflowException("The required parameter 'database_id' is empty or None")

    def execute(self, context: Context) -> bool:
        hook = SpannerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        database = hook.get_database(
            project_id=self.project_id, instance_id=self.instance_id, database_id=self.database_id
        )
        if not database:
            self.log.info(
                "The Cloud Spanner database was missing: "
                "'%s' in project '%s' and instance '%s'. Assuming success.",
                self.database_id,
                self.project_id,
                self.instance_id,
            )
            return True
        return hook.delete_database(
            project_id=self.project_id, instance_id=self.instance_id, database_id=self.database_id
        )
