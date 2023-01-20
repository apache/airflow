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

from typing import TYPE_CHECKING, Sequence

from deprecated import deprecated

from airflow.compat.functools import cached_property
from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GlueCatalogPartitionSensor(BaseSensorOperator):
    """
    Waits for a partition to show up in AWS Glue Catalog.

    :param table_name: The name of the table to wait for, supports the dot
        notation (my_database.my_table)
    :param expression: The partition clause to wait for. This is passed as
        is to the AWS Glue Catalog API's get_partitions function,
        and supports SQL like notation as in ``ds='2015-01-01'
        AND type='value'`` and comparison operators as in ``"ds>=2015-01-01"``.
        See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html
        #aws-glue-api-catalog-partitions-GetPartitions
    :param aws_conn_id: ID of the Airflow connection where
        credentials and extra configuration are stored
    :param region_name: Optional aws region name (example: us-east-1). Uses region from connection
        if not specified.
    :param database_name: The name of the catalog database where the partitions reside.
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    """

    template_fields: Sequence[str] = (
        "database_name",
        "table_name",
        "expression",
    )
    ui_color = "#C5CAE9"

    def __init__(
        self,
        *,
        table_name: str,
        expression: str = "ds='{{ ds }}'",
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        database_name: str = "default",
        poke_interval: int = 60 * 3,
        **kwargs,
    ):
        super().__init__(poke_interval=poke_interval, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.table_name = table_name
        self.expression = expression
        self.database_name = database_name

    def poke(self, context: Context):
        """Checks for existence of the partition in the AWS Glue Catalog table"""
        if "." in self.table_name:
            self.database_name, self.table_name = self.table_name.split(".")
        self.log.info(
            "Poking for table %s. %s, expression %s", self.database_name, self.table_name, self.expression
        )

        return self.hook.check_for_partition(self.database_name, self.table_name, self.expression)

    @deprecated(reason="use `hook` property instead.")
    def get_hook(self) -> GlueCatalogHook:
        """Gets the GlueCatalogHook"""
        return self.hook

    @cached_property
    def hook(self) -> GlueCatalogHook:
        return GlueCatalogHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
