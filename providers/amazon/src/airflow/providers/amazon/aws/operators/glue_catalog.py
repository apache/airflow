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
"""AWS Glue Data Catalog operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from airflow.sdk import Context


class GlueCatalogCreateDatabaseOperator(AwsBaseOperator[AwsBaseHook]):
    """
    Create a database in the AWS Glue Data Catalog.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueCatalogCreateDatabaseOperator`

    :param database_name: The name of the database to create.
    :param description: A description of the database.
    :param location_uri: The location of the database (e.g. an S3 path).
    :param parameters: Key-value pairs that define properties of the database.
    :param catalog_id: The ID of the Data Catalog. Defaults to the account ID.
    :param tags: Tags to assign to the database.
    :param if_exists: Behavior when the database already exists.
        ``"fail"`` raises an error, ``"skip"`` logs and returns the database name.
    """

    aws_hook_class = AwsBaseHook
    template_fields: tuple[str, ...] = (
        *AwsBaseOperator.template_fields,
        "database_name",
        "description",
        "location_uri",
    )

    def __init__(
        self,
        *,
        database_name: str,
        description: str | None = None,
        location_uri: str | None = None,
        parameters: dict[str, str] | None = None,
        catalog_id: str | None = None,
        tags: dict[str, str] | None = None,
        if_exists: Literal["fail", "skip"] = "skip",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.database_name = database_name
        self.description = description
        self.location_uri = location_uri
        self.parameters = parameters
        self.catalog_id = catalog_id
        self.tags = tags
        self.if_exists = if_exists

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        return {**super()._hook_parameters, "client_type": "glue"}

    def execute(self, context: Context) -> str:
        database_input: dict[str, Any] = prune_dict(
            {
                "Name": self.database_name,
                "Description": self.description,
                "LocationUri": self.location_uri,
                "Parameters": self.parameters,
            }
        )
        kwargs: dict[str, Any] = prune_dict(
            {
                "DatabaseInput": database_input,
                "CatalogId": self.catalog_id,
                "Tags": self.tags,
            }
        )
        try:
            self.hook.conn.create_database(**kwargs)
        except ClientError as e:
            if e.response["Error"]["Code"] == "AlreadyExistsException" and self.if_exists == "skip":
                self.log.info("Database %s already exists, skipping.", self.database_name)
            else:
                raise
        else:
            self.log.info("Created Glue Catalog database: %s", self.database_name)
        return self.database_name


class GlueCatalogDeleteDatabaseOperator(AwsBaseOperator[AwsBaseHook]):
    """
    Delete a database from the AWS Glue Data Catalog.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueCatalogDeleteDatabaseOperator`

    :param database_name: The name of the database to delete.
    :param catalog_id: The ID of the Data Catalog. Defaults to the account ID.
    """

    aws_hook_class = AwsBaseHook
    template_fields: tuple[str, ...] = (
        *AwsBaseOperator.template_fields,
        "database_name",
    )

    def __init__(
        self,
        *,
        database_name: str,
        catalog_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.database_name = database_name
        self.catalog_id = catalog_id

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        return {**super()._hook_parameters, "client_type": "glue"}

    def execute(self, context: Context) -> None:
        kwargs: dict[str, Any] = prune_dict(
            {
                "Name": self.database_name,
                "CatalogId": self.catalog_id,
            }
        )
        self.hook.conn.delete_database(**kwargs)
        self.log.info("Deleted Glue Catalog database: %s", self.database_name)


class GlueCatalogCreateTableOperator(AwsBaseOperator[AwsBaseHook]):
    """
    Create a table in an AWS Glue Data Catalog database.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueCatalogCreateTableOperator`

    :param database_name: The name of the database. (templated)
    :param table_name: The name of the table. (templated)
    :param table_input: The ``TableInput`` dict defining the table schema, storage, etc. (templated)
    :param catalog_id: The ID of the Data Catalog. Defaults to the account ID. (templated)
    :param if_exists: Behavior when the table already exists.
        ``"fail"`` raises an error, ``"skip"`` logs and returns.
    """

    aws_hook_class = AwsBaseHook
    template_fields: tuple[str, ...] = (
        *AwsBaseOperator.template_fields,
        "database_name",
        "table_name",
        "catalog_id",
    )
    template_fields_renderers = {"table_input": "json"}

    def __init__(
        self,
        *,
        database_name: str,
        table_name: str,
        table_input: dict[str, Any],
        catalog_id: str | None = None,
        if_exists: Literal["fail", "skip"] = "skip",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.database_name = database_name
        self.table_name = table_name
        self.table_input = table_input
        self.catalog_id = catalog_id
        self.if_exists = if_exists

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        return {**super()._hook_parameters, "client_type": "glue"}

    def execute(self, context: Context) -> str:
        self.log.info("Creating Glue table %s in database %s", self.table_name, self.database_name)
        # Ensure Name is set in TableInput
        table_input = {**self.table_input, "Name": self.table_name}
        kwargs: dict[str, Any] = prune_dict(
            {
                "DatabaseName": self.database_name,
                "TableInput": table_input,
                "CatalogId": self.catalog_id,
            }
        )
        try:
            self.hook.conn.create_table(**kwargs)
        except ClientError as e:
            if e.response["Error"]["Code"] == "AlreadyExistsException" and self.if_exists == "skip":
                self.log.info("Table %s already exists, skipping.", self.table_name)
            else:
                raise
        self.log.info("Table %s created.", self.table_name)
        return self.table_name


class GlueCatalogDeleteTableOperator(AwsBaseOperator[AwsBaseHook]):
    """
    Delete a table from an AWS Glue Data Catalog database.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueCatalogDeleteTableOperator`

    :param database_name: The name of the database. (templated)
    :param table_name: The name of the table to delete. (templated)
    :param catalog_id: The ID of the Data Catalog. Defaults to the account ID. (templated)
    """

    aws_hook_class = AwsBaseHook
    template_fields: tuple[str, ...] = (
        *AwsBaseOperator.template_fields,
        "database_name",
        "table_name",
        "catalog_id",
    )

    def __init__(
        self,
        *,
        database_name: str,
        table_name: str,
        catalog_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.database_name = database_name
        self.table_name = table_name
        self.catalog_id = catalog_id

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        return {**super()._hook_parameters, "client_type": "glue"}

    def execute(self, context: Context) -> None:
        self.log.info("Deleting Glue table %s from database %s", self.table_name, self.database_name)
        kwargs: dict[str, Any] = prune_dict(
            {
                "DatabaseName": self.database_name,
                "Name": self.table_name,
                "CatalogId": self.catalog_id,
            }
        )
        self.hook.conn.delete_table(**kwargs)
        self.log.info("Deleted table %s", self.table_name)
