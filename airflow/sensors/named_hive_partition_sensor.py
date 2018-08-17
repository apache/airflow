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

from past.builtins import basestring

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class NamedHivePartitionSensor(BaseSensorOperator):
    """
    Waits for a set of partitions to show up in Hive.

    :param partition_names: List of fully qualified names of the
        partitions to wait for. A fully qualified name is of the
        form ``schema.table/pk1=pv1/pk2=pv2``, for example,
        default.users/ds=2016-01-01. This is passed as is to the metastore
        Thrift client ``get_partitions_by_name`` method. Note that
        you cannot use logical or comparison operators as in
        HivePartitionSensor.
    :type partition_names: list of strings
    :param metastore_conn_id: reference to the metastore thrift service
        connection id
    :type metastore_conn_id: str
    """

    template_fields = ('partition_names',)
    ui_color = '#8d99ae'

    @apply_defaults
    def __init__(self,
                 partition_names,
                 metastore_conn_id='metastore_default',
                 poke_interval=60 * 3,
                 hook=None,
                 *args,
                 **kwargs):
        super(NamedHivePartitionSensor, self).__init__(
            poke_interval=poke_interval, *args, **kwargs)

        if isinstance(partition_names, basestring):
            raise TypeError('partition_names must be an array of strings')

        self.metastore_conn_id = metastore_conn_id
        self.partition_names = partition_names
        self.hook = hook
        if self.hook and metastore_conn_id != 'metastore_default':
            self.log.warning('A hook was passed but a non default'
                             'metastore_conn_id='
                             '{} was used'.format(metastore_conn_id))

    @staticmethod
    def parse_partition_name(partition):
        first_split = partition.split('.', 1)
        if len(first_split) == 1:
            schema = 'default'
            table_partition = max(first_split)  # poor man first
        else:
            schema, table_partition = first_split
        second_split = table_partition.split('/', 1)
        if len(second_split) == 1:
            raise ValueError('Could not parse ' + partition +
                             'into table, partition')
        else:
            table, partition = second_split
        return schema, table, partition

    def poke_partition(self, partition):
        if not self.hook:
            from airflow.hooks.hive_hooks import HiveMetastoreHook
            self.hook = HiveMetastoreHook(
                metastore_conn_id=self.metastore_conn_id)

        schema, table, partition = self.parse_partition_name(partition)

        self.log.info(
            'Poking for {schema}.{table}/{partition}'.format(**locals())
        )
        return self.hook.check_for_named_partition(
            schema, table, partition)

    def poke(self, context):

        self.partition_names = [
            partition_name for partition_name in self.partition_names
            if not self.poke_partition(partition_name)
        ]
        return not self.partition_names
