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

from airflow.providers.apache.hive.sensors.hive_partition import HivePartitionSensor
from airflow.providers.cloudera.hooks.cdw_hook import CdwHiveMetastoreHook


class CdwHivePartitionSensor(HivePartitionSensor):
    """
    CdwHivePartitionSensor is a subclass of HivePartitionSensor and supposed to implement
    the same logic by delegating the actual work to a CdwHiveMetastoreHook instance.
    """

    template_fields = (
        "schema",
        "table",
        "partition",
    )
    ui_color = "#C5CAE9"

    def __init__(
        self,
        table,
        partition="ds='{{ ds }}'",
        cli_conn_id="metastore_default",
        schema="default",
        poke_interval=60 * 3,
        *args,
        **kwargs,
    ):
        super().__init__(table=table, poke_interval=poke_interval, *args, **kwargs)
        if not partition:
            partition = "ds='{{ ds }}'"
        self.cli_conn_id = cli_conn_id
        self.table = table
        self.partition = partition
        self.schema = schema
        self.hook = None

    def poke(self, context):
        if "." in self.table:
            self.schema, self.table = self.table.split(".")
        self.log.info(
            "Poking for table %s.%s, partition %s",
            self.schema,
            self.table,
            self.partition,
        )
        if self.hook is None:
            self.hook = CdwHiveMetastoreHook(cli_conn_id=self.cli_conn_id)
        return self.hook.check_for_partition(self.schema, self.table, self.partition)
