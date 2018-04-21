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

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.aws_hook import AwsHook


class DMSHook(AwsHook):
    """
    Interact with AWS Database Migration Service.

    :param dms_conn_id: The connection to AWS DMS, only necessary for
    the create_replication_task method
    :param dms_conn_id: string
    """

    def __init__(self, dms_conn_id=None, *args, **kwargs):
        self.dms_conn_id = dms_conn_id
        super(DMSHook, self).__init__(*args, **kwargs)

    def get_conn(self):
        self.conn = self.get_client_type('dms')
        return self.conn

    def create_replication_task(self, replication_task_overrides):
        """
        Creates a replication task using the config from the DMS connection.
        Keys of the json extra hash may have the arguments of the boto3
        create_replication_task method. Overrides for this config may be
        passed as the replication_task_overrides.
        """

        if not self.dms_conn_id:
            raise AirflowException('dms_conn_id must be present '
                                   'to use create_replication_task')

        dms_conn = self.get_connection(self.dms_conn_id)

        config = dms_conn.extra_dejson.copy()
        config.update(replication_task_overrides)

        return self.get_conn().create_replication_task(
            ReplicationTaskIdentifier=config.get('replication_task_identifier'),
            SourceEndpointArn=config.get('source_endpoint_arn'),
            TargetEndpointArn=config.get('target_endpoint_arn'),
            ReplicationInstanceArn=config.get('replication_instance_arn'),
            MigrationType=config.get('migration_type'),
            TableMappings=config.get('table_mappings'),
            ReplicationTaskSettings=config.get('replication_task_settings')
        )

    def stop_replication_task(self, replication_task_arn):
        return self.get_conn().stop_replication_task(
            ReplicationTaskArn=replication_task_arn
        )

    def delete_replication_task(self, replication_task_arn):
        return self.get_conn().delete_replication_task(
            ReplicationTaskArn=replication_task_arn
        )

    def start_replication_task(self, replication_task_arn, start_replication_task_type, cdc_start_time=None):
        return self.get_conn().start_replication_task(
            ReplicationTaskArn=replication_task_arn,
            StartReplicationTaskType=start_replication_task_type
            # ,CdcStartTime=self.cdc_start_time,
        )

    def describe_replication_task(self, replication_task_arn):
        return self.get_conn().describe_replication_tasks(
            Filters=[
                {
                    'Name': 'replication-task-arn',
                    'Values': [
                        replication_task_arn,
                    ]
                },
            ],
        )
