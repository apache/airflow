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

from airflow.contrib.hooks.redshift_hook import RedshiftHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class AwsRedshiftClusterSensor(BaseSensorOperator):
    """
    Waits for a Redshift cluster to reach a specific status.

    :param cluster_identifier: The identifier for the cluster being pinged.
    :type cluster_identifier: str
    :param target_status: The cluster status desired.
    :type target_status: str
    """
    template_fields = ('cluster_identifier', 'target_status')

    @apply_defaults
    def __init__(self,
                 cluster_identifier,
                 target_status='available',
                 aws_conn_id='aws_default',
                 *args,
                 **kwargs):
        super(AwsRedshiftClusterSensor, self).__init__(*args, **kwargs)
        self.cluster_identifier = cluster_identifier
        self.target_status = target_status
        self.aws_conn_id = aws_conn_id

    def poke(self, context):
        self.log.info('Poking for status : %s\nfor cluster %s',
                      self.target_status, self.cluster_identifier)
        hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        return hook.cluster_status(self.cluster_identifier) == self.target_status
