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
"""This module contains AWS Glue Catalog Hook."""

from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class GlueCatalogHook(AwsBaseHook):
    """
    Interact with AWS Glue Data Catalog.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("glue") <Glue.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
        - `AWS Glue Data Catalog \
        <https://docs.aws.amazon.com/glue/latest/dg/components-overview.html#data-catalog-intro>`__
    """

    def __init__(self, *args, **kwargs):
        super().__init__(client_type="glue", *args, **kwargs)

    async def async_get_partitions(
        self,
        client: Any,
        database_name: str,
        table_name: str,
        expression: str = "",
        page_size: int | None = None,
        max_items: int | None = 1,
    ) -> set[tuple]:
        """
        Asynchronously retrieves the partition values for a table.

        :param database_name: The name of the catalog database where the partitions reside.
        :param table_name: The name of the partitions' table.
        :param expression: An expression filtering the partitions to be returned.
            Please see official AWS documentation for further information.
            https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetPartitions
        :param page_size: pagination size
        :param max_items: maximum items to return
        :return: set of partition values where each value is a tuple since
            a partition may be composed of multiple columns. For example:
            ``{('2018-01-01','1'), ('2018-01-01','2')}``
        """
        config = {
            "PageSize": page_size,
            "MaxItems": max_items,
        }

        paginator = client.get_paginator("get_partitions")
        partitions = set()

        async for page in paginator.paginate(
            DatabaseName=database_name,
            TableName=table_name,
            Expression=expression,
            PaginationConfig=config,
        ):
            for partition in page["Partitions"]:
                partitions.add(tuple(partition["Values"]))

        return partitions

    def get_partitions(
        self,
        database_name: str,
        table_name: str,
        expression: str = "",
        page_size: int | None = None,
        max_items: int | None = None,
    ) -> set[tuple]:
        """
        Retrieve the partition values for a table.

        .. seealso::
            - :external+boto3:py:class:`Glue.Paginator.GetPartitions`

        :param database_name: The name of the catalog database where the partitions reside.
        :param table_name: The name of the partitions' table.
        :param expression: An expression filtering the partitions to be returned.
            Please see official AWS documentation for further information.
            https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetPartitions
        :param page_size: pagination size
        :param max_items: maximum items to return
        :return: set of partition values where each value is a tuple since
            a partition may be composed of multiple columns. For example:
            ``{('2018-01-01','1'), ('2018-01-01','2')}``
        """
        config = {
            "PageSize": page_size,
            "MaxItems": max_items,
        }

        paginator = self.get_conn().get_paginator("get_partitions")
        response = paginator.paginate(
            DatabaseName=database_name,
            TableName=table_name,
            Expression=expression,
            PaginationConfig=config,
        )

        partitions = set()
        for page in response:
            for partition in page["Partitions"]:
                partitions.add(tuple(partition["Values"]))

        return partitions

    def check_for_partition(
        self, database_name: str, table_name: str, expression: str
    ) -> bool:
        """
        Check whether a partition exists.

        .. code-block:: python

            hook = GlueCatalogHook()
            t = "static_babynames_partitioned"
            hook.check_for_partition("airflow", t, "ds='2015-01-01'")

        :param database_name: Name of hive database (schema) @table belongs to
        :param table_name: Name of hive table @partition belongs to
        :expression: Expression that matches the partitions to check for, e.g.: ``a = 'b' AND c = 'd'``
        """
        partitions = self.get_partitions(
            database_name, table_name, expression, max_items=1
        )

        return bool(partitions)

    def get_table(self, database_name: str, table_name: str) -> dict:
        """
        Get the information of the table.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.get_table`

        .. code-block:: python

            hook = GlueCatalogHook()
            r = hook.get_table("db", "table_foo")
            r["Name"] = "table_foo"

        :param database_name: Name of hive database (schema) @table belongs to
        :param table_name: Name of hive table
        """
        result = self.get_conn().get_table(DatabaseName=database_name, Name=table_name)

        return result["Table"]

    def get_table_location(self, database_name: str, table_name: str) -> str:
        """
        Get the physical location of the table.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.get_table`

        :param database_name: Name of hive database (schema) @table belongs to
        :param table_name: Name of hive table
        """
        table = self.get_table(database_name, table_name)

        return table["StorageDescriptor"]["Location"]

    def get_partition(
        self, database_name: str, table_name: str, partition_values: list[str]
    ) -> dict:
        """
        Get a Partition.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.get_partition`

        .. code-block:: python

            hook = GlueCatalogHook()
            partition = hook.get_partition("db", "table", ["string"])
            partition["Values"]

        :param database_name: Database name
        :param table_name: Database's Table name
        :param partition_values: List of utf-8 strings that define the partition
            Please see official AWS documentation for further information.
            https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetPartition
        :raises: AirflowException
        """
        try:
            response = self.get_conn().get_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionValues=partition_values,
            )
            return response["Partition"]
        except ClientError as e:
            self.log.error("Client error: %s", e)
            raise AirflowException("AWS request failed, check logs for more info")

    def create_partition(
        self, database_name: str, table_name: str, partition_input: dict
    ) -> dict:
        """
        Create a new Partition.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.create_partition`

        .. code-block:: python

            hook = GlueCatalogHook()
            partition_input = {"Values": []}
            hook.create_partition(
                database_name="db", table_name="table", partition_input=partition_input
            )

        :param database_name: Database name
        :param table_name: Database's Table name
        :param partition_input: Definition of how the partition is created
            Please see official AWS documentation for further information.
            https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-CreatePartition
        :raises: AirflowException
        """
        try:
            return self.get_conn().create_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionInput=partition_input,
            )
        except ClientError as e:
            self.log.error("Client error: %s", e)
            raise AirflowException("AWS request failed, check logs for more info")
