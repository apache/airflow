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

from functools import cached_property
from typing import Any, Sequence

from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.kyuubi.hooks.kyuubi import KyuubiHook


class KyuubiOperator(HiveOperator):
    """
    Executes hql code or hive script using Kyuubi.

    :param hql: the hql to be executed.
    :param kyuubi_conn_id: Reference to the Kyuubi connection id.
    """

    template_fields: Sequence[str] = (
        "hql",
        "schema",
        "hive_cli_conn_id",
        "mapred_queue",
        "hiveconfs",
        "mapred_job_name",
        "mapred_queue_priority",
        "proxy_user",
    )
    ui_color = "#f0ede4"

    def __init__(
        self,
        *,
        kyuubi_conn_id: str = "kyuubi_default",
        spark_queue: str | None = None,
        spark_app_name: str | None = None,
        spark_sql_shuffle_partitions: str | None = None,
        spark_conf: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        # Pass kyuubi_conn_id as hive_cli_conn_id to parent
        super().__init__(hive_cli_conn_id=kyuubi_conn_id, **kwargs)
        self.kyuubi_conn_id = kyuubi_conn_id
        self.spark_queue = spark_queue
        self.spark_app_name = spark_app_name
        self.spark_sql_shuffle_partitions = spark_sql_shuffle_partitions
        self.spark_conf = spark_conf

    @cached_property
    def hook(self) -> KyuubiHook:
        """Get Kyuubi hook."""
        return KyuubiHook(
            kyuubi_conn_id=self.kyuubi_conn_id,
            spark_queue=self.spark_queue,
            spark_app_name=self.spark_app_name,
            spark_sql_shuffle_partitions=self.spark_sql_shuffle_partitions,
            spark_conf=self.spark_conf,
            mapred_queue_priority=self.mapred_queue_priority,
            hive_cli_params=self.hive_cli_params,
            auth=self.auth,
            proxy_user=self.proxy_user,
        )
