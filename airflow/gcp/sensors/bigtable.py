# -*- coding: utf-8 -*-
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
"""
This module contains Google Cloud Bigtable sensor.
"""
from typing import Optional

import google.api_core.exceptions
from google.cloud.bigtable.table import ClusterState
from google.cloud.bigtable_admin_v2 import enums

from airflow.gcp.hooks.bigtable import BigtableHook
from airflow.gcp.operators.bigtable import BigtableValidationMixin
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class BigtableTableReplicationCompletedSensor(BaseSensorOperator, BigtableValidationMixin):
    """
    Sensor that waits for Cloud Bigtable table to be fully replicated to its clusters.
    No exception will be raised if the instance or the table does not exist.

    For more details about cluster states for a table, have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/table.html#google.cloud.bigtable.table.Table.get_cluster_states

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigtableTableReplicationCompletedSensor`

    :type instance_id: str
    :param instance_id: The ID of the Cloud Bigtable instance.
    :type table_id: str
    :param table_id: The ID of the table to check replication status.
    :type project_id: str
    :param project_id: Optional, the ID of the GCP project.
    """
    REQUIRED_ATTRIBUTES = ('instance_id', 'table_id')
    template_fields = ['project_id', 'instance_id', 'table_id']

    @apply_defaults
    def __init__(
        self,
        instance_id: str,
        table_id: str,
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        *args,
        **kwargs
    ) -> None:
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        super().__init__(*args, **kwargs)

    def poke(self, context):
        hook = BigtableHook(gcp_conn_id=self.gcp_conn_id)
        instance = hook.get_instance(project_id=self.project_id, instance_id=self.instance_id)
        if not instance:
            self.log.info("Dependency: instance '%s' does not exist.", self.instance_id)
            return False

        try:
            cluster_states = hook.get_cluster_states_for_table(instance=instance, table_id=self.table_id)
        except google.api_core.exceptions.NotFound:
            self.log.info(
                "Dependency: table '%s' does not exist in instance '%s'.",
                self.table_id, self.instance_id)
            return False

        ready_state = ClusterState(enums.Table.ClusterState.ReplicationState.READY)

        is_table_replicated = True
        for cluster_id in cluster_states.keys():
            if cluster_states[cluster_id] != ready_state:
                self.log.info("Table '%s' is not yet replicated on cluster '%s'.",
                              self.table_id, cluster_id)
                is_table_replicated = False

        if not is_table_replicated:
            return False

        self.log.info("Table '%s' is replicated.", self.table_id)
        return True
