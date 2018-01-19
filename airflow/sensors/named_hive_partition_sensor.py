# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
                 *args,
                 **kwargs):
        super(NamedHivePartitionSensor, self).__init__(
            poke_interval=poke_interval, *args, **kwargs)

        if isinstance(partition_names, basestring):
            raise TypeError('partition_names must be an array of strings')

        self.metastore_conn_id = metastore_conn_id
        self.partition_names = partition_names
        self.next_poke_idx = 0

    @classmethod
    def parse_partition_name(self, partition):
        try:
            schema, table_partition = partition.split('.', 1)
            table, partition = table_partition.split('/', 1)
            return schema, table, partition
        except ValueError as e:
            raise ValueError('Could not parse ' + partition)

    def poke(self, context):
        if not hasattr(self, 'hook'):
            from airflow.hooks.hive_hooks import HiveMetastoreHook
            self.hook = HiveMetastoreHook(
                metastore_conn_id=self.metastore_conn_id)

        def poke_partition(partition):

            schema, table, partition = self.parse_partition_name(partition)

            self.log.info(
                'Poking for {schema}.{table}/{partition}'.format(**locals())
            )
            return self.hook.check_for_named_partition(
                schema, table, partition)

        while self.next_poke_idx < len(self.partition_names):
            if poke_partition(self.partition_names[self.next_poke_idx]):
                self.next_poke_idx += 1
            else:
                return False

        return True
