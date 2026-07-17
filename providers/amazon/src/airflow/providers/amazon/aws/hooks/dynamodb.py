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
"""This module contains the Amazon DynamoDB Hook."""

from __future__ import annotations

from collections.abc import Iterable
from functools import cached_property
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from botocore.client import BaseClient


class DynamoDBHook(AwsBaseHook):
    """
    Interact with Amazon DynamoDB.

    Provide thick wrapper around
    :external+boto3:py:class:`boto3.resource("dynamodb") <DynamoDB.ServiceResource>`.

    :param table_keys: partition key and sort key
    :param table_name: target DynamoDB table

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(
        self, *args, table_keys: list | None = None, table_name: str | None = None, **kwargs
    ) -> None:
        self.table_keys = table_keys
        self.table_name = table_name
        kwargs["resource_type"] = "dynamodb"
        super().__init__(*args, **kwargs)

    @cached_property
    def client(self) -> BaseClient:
        """Return boto3 client."""
        return self.get_conn().meta.client

    def write_batch_data(self, items: Iterable) -> bool:
        """
        Write batch items to DynamoDB table with provisioned throughout capacity.

        .. seealso::
            - :external+boto3:py:meth:`DynamoDB.ServiceResource.Table`
            - :external+boto3:py:meth:`DynamoDB.Table.batch_writer`
            - :external+boto3:py:meth:`DynamoDB.Table.put_item`

        :param items: list of DynamoDB items.
        """
        try:
            self.log.debug("Loading table %s from DynamoDB connection", self.table_name)
            table = self.get_conn().Table(self.table_name)

            self.log.debug(
                "Starting batch write to table %s with overwrite keys %s",
                self.table_name,
                self.table_keys,
            )
            with table.batch_writer(overwrite_by_pkeys=self.table_keys) as batch:
                for item in items:
                    batch.put_item(Item=item)
            self.log.debug("Batch write to table %s completed successfully", self.table_name)
            return True
        except Exception as e:
            raise AirflowException(f"Failed to insert items in dynamodb, error: {e}") from e

    def get_import_status(self, import_arn: str) -> tuple[str, str | None, str | None]:
        """
        Get import status from Dynamodb.

        :param import_arn: The Amazon Resource Name (ARN) for the import.
        :return: Import status, Error code and Error message
        """
        self.log.info("Poking for Dynamodb import %s", import_arn)

        try:
            self.log.debug("Calling describe_import for import ARN %s", import_arn)
            response = self.client.describe_import(ImportArn=import_arn)
            status = response["ImportTableDescription"]["ImportStatus"]
            error_code = response["ImportTableDescription"].get("FailureCode")
            error_msg = response["ImportTableDescription"].get("FailureMessage")
            self.log.debug(
                "Import %s status: %s, error_code: %s, error_message: %s",
                import_arn,
                status,
                error_code,
                error_msg,
            )
            return status, error_code, error_msg
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "ImportNotFoundException":
                raise AirflowException(
                    f"S3 import into Dynamodb job not found. Import ARN: {import_arn}"
                ) from e
            raise

    def get_item(
        self,
        table_name: str,
        key: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Retrieve a single item from a DynamoDB table by primary key.

        Uses the boto3 resource API so keys and attribute values are plain
        Python types (str, int, Decimal, …) rather than the low-level typed
        format (``{"S": "value"}``).

        .. seealso::
            - :external+boto3:py:meth:`DynamoDB.Table.get_item`

        :param table_name: Name of the DynamoDB table.
        :param key: Primary key of the item, e.g. ``{"pk": "value"}`` or
            ``{"pk": "value", "sk": "range_value"}``.
        :return: The item as a plain dict, or ``None`` if not found.
        """
        self.log.debug("Getting item with key %s from table %s", key, table_name)
        try:
            table = self.get_conn().Table(table_name)
            response = table.get_item(Key=key)
            item = response.get("Item")
            if item is None:
                self.log.info("Item with key %s not found in table %s", key, table_name)
            return item
        except ClientError as e:
            self.log.error("Failed to get item from %s: %s", table_name, e)
            raise

    def put_item(
        self,
        table_name: str,
        item: dict[str, Any],
        condition_expression: str | None = None,
    ) -> dict[str, Any]:
        """
        Insert or replace an item in a DynamoDB table.

        .. seealso::
            - :external+boto3:py:meth:`DynamoDB.Table.put_item`

        :param table_name: Name of the DynamoDB table.
        :param item: Item attributes as a plain dict,
            e.g. ``{"pk": "value", "status": "pending"}``.
        :param condition_expression: Optional condition expression string.
        :return: The raw response from DynamoDB.
        """
        self.log.debug("Putting item into table %s", table_name)
        try:
            table = self.get_conn().Table(table_name)
            params: dict[str, Any] = {"Item": item}
            if condition_expression:
                params["ConditionExpression"] = condition_expression
            response = table.put_item(**params)
            self.log.info("Successfully put item into table %s", table_name)
            return response
        except ClientError as e:
            self.log.error("Failed to put item into %s: %s", table_name, e)
            raise

    def update_item(
        self,
        table_name: str,
        key: dict[str, Any],
        update_expression: str,
        expression_attribute_values: dict[str, Any],
        expression_attribute_names: dict[str, str] | None = None,
        condition_expression: str | None = None,
    ) -> dict[str, Any] | None:
        """
        Update attributes of an existing item in a DynamoDB table.

        Uses the boto3 resource API so values are plain Python types.

        .. seealso::
            - :external+boto3:py:meth:`DynamoDB.Table.update_item`

        :param table_name: Name of the DynamoDB table.
        :param key: Primary key of the item to update.
        :param update_expression: Update expression, e.g.
            ``"SET #s = :status, updated_at = :ts"``.
        :param expression_attribute_values: Substitution values for the
            expression, e.g. ``{":status": "done", ":ts": "2024-01-01"}``.
        :param expression_attribute_names: Optional name aliases for reserved
            words, e.g. ``{"#s": "status"}``.
        :param condition_expression: Optional condition expression string.
        :return: The updated item attributes, or ``None`` if the update
            returned no attributes.
        """
        self.log.debug("Updating item with key %s in table %s", key, table_name)
        try:
            table = self.get_conn().Table(table_name)
            params: dict[str, Any] = {
                "Key": key,
                "UpdateExpression": update_expression,
                "ExpressionAttributeValues": expression_attribute_values,
                "ReturnValues": "ALL_NEW",
            }
            if expression_attribute_names:
                params["ExpressionAttributeNames"] = expression_attribute_names
            if condition_expression:
                params["ConditionExpression"] = condition_expression
            response = table.update_item(**params)
            self.log.info("Successfully updated item in table %s", table_name)
            return response.get("Attributes")
        except ClientError as e:
            self.log.error("Failed to update item in %s: %s", table_name, e)
            raise

    def delete_item(
        self,
        table_name: str,
        key: dict[str, Any],
        condition_expression: str | None = None,
    ) -> dict[str, Any] | None:
        """
        Delete an item from a DynamoDB table.

        .. seealso::
            - :external+boto3:py:meth:`DynamoDB.Table.delete_item`

        :param table_name: Name of the DynamoDB table.
        :param key: Primary key of the item to delete.
        :param condition_expression: Optional condition expression string.
        :return: The deleted item's attributes, or ``None`` if the item did
            not exist.
        """
        self.log.debug("Deleting item with key %s from table %s", key, table_name)
        try:
            table = self.get_conn().Table(table_name)
            params: dict[str, Any] = {"Key": key, "ReturnValues": "ALL_OLD"}
            if condition_expression:
                params["ConditionExpression"] = condition_expression
            response = table.delete_item(**params)
            self.log.info("Successfully deleted item from table %s", table_name)
            return response.get("Attributes")
        except ClientError as e:
            self.log.error("Failed to delete item from %s: %s", table_name, e)
            raise

    def query(
        self,
        table_name: str,
        key_condition_expression: Any,
        expression_attribute_values: dict[str, Any] | None = None,
        expression_attribute_names: dict[str, str] | None = None,
        filter_expression: Any | None = None,
        index_name: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Query items from a DynamoDB table or secondary index.

        Uses the boto3 resource API so values are plain Python types.
        ``key_condition_expression`` accepts either a string expression or a
        boto3 :py:class:`boto3.dynamodb.conditions.ConditionBase` object
        (e.g. ``Key("pk").eq("value")``).

        .. seealso::
            - :external+boto3:py:meth:`DynamoDB.Table.query`
            - :external+boto3:py:class:`boto3.dynamodb.conditions.Key`

        :param table_name: Name of the DynamoDB table.
        :param key_condition_expression: Key condition — string or
            ``boto3.dynamodb.conditions.ConditionBase``.
        :param expression_attribute_values: Substitution values (required when
            using string expressions, omit when using condition objects).
        :param expression_attribute_names: Optional name aliases for reserved
            words.
        :param filter_expression: Optional filter applied after the query —
            string or ``boto3.dynamodb.conditions.ConditionBase``.
        :param index_name: Name of a secondary index to query.
        :param limit: Maximum number of items to evaluate (see DynamoDB
            ``Limit`` semantics — this is not a guaranteed page size).
        :return: List of matching items as plain dicts.
        """
        self.log.debug("Querying table %s", table_name)
        try:
            table = self.get_conn().Table(table_name)
            params: dict[str, Any] = {"KeyConditionExpression": key_condition_expression}
            if expression_attribute_values:
                params["ExpressionAttributeValues"] = expression_attribute_values
            if expression_attribute_names:
                params["ExpressionAttributeNames"] = expression_attribute_names
            if filter_expression is not None:
                params["FilterExpression"] = filter_expression
            if index_name:
                params["IndexName"] = index_name
            if limit:
                params["Limit"] = limit
            response = table.query(**params)
            items = response.get("Items", [])
            self.log.info("Query on table %s returned %d items", table_name, len(items))
            return items
        except ClientError as e:
            self.log.error("Failed to query table %s: %s", table_name, e)
            raise
