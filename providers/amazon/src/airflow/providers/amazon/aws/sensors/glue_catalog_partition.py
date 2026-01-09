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

from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.triggers.glue import GlueCatalogPartitionTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException, conf

if TYPE_CHECKING:
    from airflow.sdk import Context


class GlueCatalogPartitionSensor(AwsBaseSensor[GlueCatalogHook]):
    """
    Waits for a partition to show up in AWS Glue Catalog.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:GlueCatalogPartitionSensor`

    :param table_name: The name of the table to wait for, supports the dot
        notation (my_database.my_table)
    :param expression: The partition clause to wait for. This is passed as
        is to the AWS Glue Catalog API's get_partitions function,
        and supports SQL like notation as in ``ds='2015-01-01'
        AND type='value'`` and comparison operators as in ``"ds>=2015-01-01"``.
        See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html
        #aws-glue-api-catalog-partitions-GetPartitions
    :param database_name: The name of the catalog database where the partitions reside.
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :param deferrable: If true, then the sensor will wait asynchronously for the partition to
        show up in the AWS Glue Catalog.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = GlueCatalogHook

    template_fields: Sequence[str] = aws_template_fields(
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
        database_name: str = "default",
        poke_interval: int = 60 * 3,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.expression = expression
        self.database_name = database_name
        self.poke_interval = poke_interval
        self.deferrable = deferrable

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=GlueCatalogPartitionTrigger(
                    database_name=self.database_name,
                    table_name=self.table_name,
                    expression=self.expression,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                    waiter_delay=int(self.poke_interval),
                    verify=self.verify,
                    botocore_config=self.botocore_config,
                ),
                method_name="execute_complete",
                timeout=timedelta(seconds=self.timeout),
            )
        else:
            super().execute(context=context)

    def poke(self, context: Context):
        """Check for existence of the partition in the AWS Glue Catalog table."""
        if "." in self.table_name:
            self.database_name, self.table_name = self.table_name.split(".")
        self.log.info(
            "Poking for table %s. %s, expression %s", self.database_name, self.table_name, self.expression
        )

        return self.hook.check_for_partition(self.database_name, self.table_name, self.expression)

    def execute_complete(self, context: Context, event: dict | None = None) -> None:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(f"Trigger error: event is {validated_event}")
        self.log.info("Partition exists in the Glue Catalog")
