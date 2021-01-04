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
from typing import Optional

from airflow.providers.amazon.aws.hooks.glue_catalog import AwsGlueCatalogHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class AwsGlueCatalogPartitionSensor(BaseSensorOperator):
    """
    Waits for a partition to show up in AWS Glue Catalog.

    :param table_name: The name of the table to wait for, supports the dot
        notation (my_database.my_table)
    :type table_name: str
    :param expression: The partition clause to wait for. This is passed as
        is to the AWS Glue Catalog API's get_partitions function,
        and supports SQL like notation as in ``ds='2015-01-01'
        AND type='value'`` and comparison operators as in ``"ds>=2015-01-01"``.
        See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html
        #aws-glue-api-catalog-partitions-GetPartitions
    :type expression: str
    :param aws_conn_id: ID of the Airflow connection where
        credentials and extra configuration are stored
    :type aws_conn_id: str
    :param region_name: Optional aws region name (example: us-east-1). Uses region from connection
        if not specified.
    :type region_name: str
    :param database_name: The name of the catalog database where the partitions reside.
    :type database_name: str
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: int
    """

    template_fields = (
        'database_name',
        'table_name',
        'expression',
    )
    ui_color = '#C5CAE9'

    @apply_defaults
    def __init__(
        self,
        *,
        table_name: str,
        expression: str = "ds='{{ ds }}'",
        aws_conn_id: str = 'aws_default',
        region_name: Optional[str] = None,
        database_name: str = 'default',
        poke_interval: int = 60 * 3,
        **kwargs,
    ):
        super().__init__(poke_interval=poke_interval, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.table_name = table_name
        self.expression = expression
        self.database_name = database_name
        self.hook: Optional[AwsGlueCatalogHook] = None

    def poke(self, context):
        """Checks for existence of the partition in the AWS Glue Catalog table"""
        if '.' in self.table_name:
            self.database_name, self.table_name = self.table_name.split('.')
        self.log.info(
            'Poking for table %s. %s, expression %s', self.database_name, self.table_name, self.expression
        )

        return self.get_hook().check_for_partition(self.database_name, self.table_name, self.expression)

    def get_hook(self) -> AwsGlueCatalogHook:
        """Gets the AwsGlueCatalogHook"""
        if self.hook:
            return self.hook

        self.hook = AwsGlueCatalogHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
        return self.hook
