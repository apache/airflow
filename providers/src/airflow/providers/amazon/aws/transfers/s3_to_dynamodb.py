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
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Sequence, TypedDict

from botocore.exceptions import ClientError, WaiterError

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AttributeDefinition(TypedDict):
    """Attribute Definition Type."""

    AttributeName: str
    AttributeType: Literal["S", "N", "B"]


class KeySchema(TypedDict):
    """Key Schema Type."""

    AttributeName: str
    KeyType: Literal["HASH", "RANGE"]


class S3ToDynamoDBOperator(BaseOperator):
    """
    Load Data from S3 into a DynamoDB.

    Data stored in S3 can be uploaded to a new or existing DynamoDB. Supported file formats CSV, DynamoDB JSON and
    Amazon ION.


    :param s3_bucket: The S3 bucket that is imported
    :param s3_key: Key prefix that imports single or multiple objects from S3
    :param dynamodb_table_name: Name of the table that shall be created
    :param dynamodb_key_schema: Primary key and sort key. Each element represents one primary key
        attribute. AttributeName is the name of the attribute. KeyType is the role for the attribute. Valid values
        HASH or RANGE
    :param dynamodb_attributes: Name of the attributes of a table. AttributeName is the name for the attribute
        AttributeType is the data type for the attribute. Valid values for AttributeType are
        S - attribute is of type String
        N - attribute is of type Number
        B - attribute is of type Binary
    :param dynamodb_tmp_table_prefix: Prefix for the temporary DynamoDB table
    :param delete_on_error: If set, the new DynamoDB table will be deleted in case of import errors
    :param use_existing_table: Whether to import to an existing non new DynamoDB table. If set to
        true data is loaded first into a temporary DynamoDB table (using the AWS ImportTable Service),
        then retrieved as chunks into memory and loaded into the target table. If set to false, a new
        DynamoDB table will be created and S3 data is bulk loaded by the AWS ImportTable Service.
    :param input_format: The format for the imported data. Valid values for InputFormat are CSV, DYNAMODB_JSON
        or ION
    :param billing_mode: Billing mode for the table. Valid values are PROVISIONED or PAY_PER_REQUEST
    :param on_demand_throughput: Extra options for maximum number of read and write units
    :param import_table_kwargs: Any additional optional import table parameters to pass, such as ClientToken,
        InputCompressionType, or InputFormatOptions. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/import_table.html
    :param import_table_creation_kwargs: Any additional optional import table creation parameters to pass, such as
        ProvisionedThroughput, SSESpecification, or GlobalSecondaryIndexes. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/import_table.html
    :param wait_for_completion: Whether to wait for cluster to stop
    :param check_interval: Time in seconds to wait between status checks
    :param max_attempts: Maximum number of attempts to check for job completion
    :param aws_conn_id: The reference to the AWS connection details
    """

    template_fields: Sequence[str] = (
        "s3_bucket",
        "s3_key",
        "dynamodb_table_name",
        "dynamodb_key_schema",
        "dynamodb_attributes",
        "dynamodb_tmp_table_prefix",
        "delete_on_error",
        "use_existing_table",
        "input_format",
        "billing_mode",
        "import_table_kwargs",
        "import_table_creation_kwargs",
    )
    ui_color = "#e2e8f0"

    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_key: str,
        dynamodb_table_name: str,
        dynamodb_key_schema: list[KeySchema],
        dynamodb_attributes: list[AttributeDefinition] | None = None,
        dynamodb_tmp_table_prefix: str = "tmp",
        delete_on_error: bool = False,
        use_existing_table: bool = False,
        input_format: Literal["CSV", "DYNAMODB_JSON", "ION"] = "DYNAMODB_JSON",
        billing_mode: Literal["PROVISIONED", "PAY_PER_REQUEST"] = "PAY_PER_REQUEST",
        import_table_kwargs: dict[str, Any] | None = None,
        import_table_creation_kwargs: dict[str, Any] | None = None,
        wait_for_completion: bool = True,
        check_interval: int = 30,
        max_attempts: int = 240,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.dynamodb_table_name = dynamodb_table_name
        self.dynamodb_attributes = dynamodb_attributes
        self.dynamodb_tmp_table_prefix = dynamodb_tmp_table_prefix
        self.delete_on_error = delete_on_error
        self.use_existing_table = use_existing_table
        self.dynamodb_key_schema = dynamodb_key_schema
        self.input_format = input_format
        self.billing_mode = billing_mode
        self.import_table_kwargs = import_table_kwargs
        self.import_table_creation_kwargs = import_table_creation_kwargs
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_attempts = max_attempts
        self.aws_conn_id = aws_conn_id

    @property
    def tmp_table_name(self):
        """Temporary table name."""
        return f"{self.dynamodb_tmp_table_prefix}_{self.dynamodb_table_name}"

    def _load_into_new_table(self, table_name: str, delete_on_error: bool) -> str:
        """
        Import S3 key or keys into a new DynamoDB table.

        :param table_name: Name of the table that shall be created
        :param delete_on_error: If set, the new DynamoDB table will be deleted in case of import errors
        :return: The Amazon resource number (ARN)
        """
        dynamodb_hook = DynamoDBHook(aws_conn_id=self.aws_conn_id)
        client = dynamodb_hook.client

        import_table_config = self.import_table_kwargs or {}
        import_table_creation_config = self.import_table_creation_kwargs or {}

        try:
            response = client.import_table(
                S3BucketSource={
                    "S3Bucket": self.s3_bucket,
                    "S3KeyPrefix": self.s3_key,
                },
                InputFormat=self.input_format,
                TableCreationParameters={
                    "TableName": table_name,
                    "AttributeDefinitions": self.dynamodb_attributes,
                    "KeySchema": self.dynamodb_key_schema,
                    "BillingMode": self.billing_mode,
                    **import_table_creation_config,
                },
                **import_table_config,
            )
        except ClientError as e:
            self.log.error("Error: failed to load from S3 into DynamoDB table. Error: %s", str(e))
            raise AirflowException(f"S3 load into DynamoDB table failed with error: {e}")

        if response["ImportTableDescription"]["ImportStatus"] == "FAILED":
            raise AirflowException(
                "S3 into Dynamodb job creation failed. Code: "
                f"{response['ImportTableDescription']['FailureCode']}. "
                f"Failure: {response['ImportTableDescription']['FailureMessage']}"
            )

        if self.wait_for_completion:
            self.log.info("Waiting for S3 into Dynamodb job to complete")
            waiter = dynamodb_hook.get_waiter("import_table")
            try:
                waiter.wait(
                    ImportArn=response["ImportTableDescription"]["ImportArn"],
                    WaiterConfig={"Delay": self.check_interval, "MaxAttempts": self.max_attempts},
                )
            except WaiterError:
                status, error_code, error_msg = dynamodb_hook.get_import_status(
                    response["ImportTableDescription"]["ImportArn"]
                )
                if delete_on_error:
                    client.delete_table(TableName=table_name)
                raise AirflowException(
                    f"S3 import into Dynamodb job failed: Status: {status}. Error: {error_code}. Error message: {error_msg}"
                )
        return response["ImportTableDescription"]["ImportArn"]

    def _load_into_existing_table(self) -> str:
        """
        Import S3 key or keys in an existing DynamoDB table.

        :return:The Amazon resource number (ARN)
        """
        if not self.wait_for_completion:
            raise ValueError("wait_for_completion must be set to True when loading into an existing table")
        table_keys = [key["AttributeName"] for key in self.dynamodb_key_schema]

        dynamodb_hook = DynamoDBHook(
            aws_conn_id=self.aws_conn_id, table_name=self.dynamodb_table_name, table_keys=table_keys
        )
        client = dynamodb_hook.client

        self.log.info("Loading from S3 into a tmp DynamoDB table %s", self.tmp_table_name)
        self._load_into_new_table(table_name=self.tmp_table_name, delete_on_error=self.delete_on_error)
        total_items = 0
        try:
            paginator = client.get_paginator("scan")
            paginate = paginator.paginate(
                TableName=self.tmp_table_name,
                Select="ALL_ATTRIBUTES",
                ReturnConsumedCapacity="NONE",
                ConsistentRead=True,
            )
            self.log.info(
                "Loading data from %s to %s DynamoDB table", self.tmp_table_name, self.dynamodb_table_name
            )
            for page in paginate:
                total_items += page.get("Count", 0)
                dynamodb_hook.write_batch_data(items=page["Items"])
            self.log.info("Number of items loaded: %s", total_items)
        except Exception as e:
            # Does not raise errors to keep previous behavior.
            self.log.warning("Error while loading data from temp table, deleting temp table. Error: %s", e)

        self.log.info("Delete tmp DynamoDB table %s", self.tmp_table_name)
        client.delete_table(TableName=self.tmp_table_name)
        return dynamodb_hook.get_conn().Table(self.dynamodb_table_name).table_arn

    def execute(self, context: Context) -> str:
        """
        Execute S3 to DynamoDB Job from Airflow.

        :param context: The current context of the task instance
        :return: The Amazon resource number (ARN)
        """
        if self.use_existing_table:
            self.log.info("Loading from S3 into new DynamoDB table %s", self.dynamodb_table_name)
            return self._load_into_existing_table()
        self.log.info("Loading from S3 into existing DynamoDB table %s", self.dynamodb_table_name)
        return self._load_into_new_table(
            table_name=self.dynamodb_table_name, delete_on_error=self.delete_on_error
        )
