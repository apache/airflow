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

import re
from time import sleep

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.aws_glue_catalog_hook import AwsGlueCatalogHook
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook

from urllib.parse import urlparse


class AWSAthenaHook(AwsHook):
    """
    Interact with AWS Athena to run, poll queries and return query results

    :param aws_conn_id: aws connection to use.
    :type aws_conn_id: str
    :param sleep_time: Time to wait between two consecutive call to check query status on athena
    :type sleep_time: int
    """

    INTERMEDIATE_STATES = ('QUEUED', 'RUNNING',)
    FAILURE_STATES = ('FAILED', 'CANCELLED',)
    SUCCESS_STATES = ('SUCCEEDED',)

    def __init__(self, aws_conn_id='aws_default', region_name=None, sleep_time=30, *args, **kwargs):
        super(AWSAthenaHook, self).__init__(aws_conn_id, **kwargs)
        self.region_name = region_name
        self.sleep_time = sleep_time
        self.conn = None

    def get_conn(self):
        """
        check if aws conn exists already or create one and return it

        :return: boto3 session
        """
        if not self.conn:
            self.conn = self.get_client_type('athena', region_name=self.region_name)
        return self.conn

    def run_query(self, query, query_context, result_configuration, client_request_token=None):
        """
        Run Presto query on athena with provided config and return submitted query_execution_id

        :param query: Presto query to run
        :type query: str
        :param query_context: Context in which query need to be run
        :type query_context: dict
        :param result_configuration: Dict with path to store results in and config related to encryption
        :type result_configuration: dict
        :param client_request_token: Unique token created by user to avoid multiple executions of same query
        :type client_request_token: str
        :return: str
        """
        response = self.conn.start_query_execution(QueryString=query,
                                                   ClientRequestToken=client_request_token,
                                                   QueryExecutionContext=query_context,
                                                   ResultConfiguration=result_configuration)
        query_execution_id = response['QueryExecutionId']
        return query_execution_id

    def check_query_status(self, query_execution_id):
        """
        Fetch the status of submitted athena query. Returns None or one of valid query states.

        :param query_execution_id: Id of submitted athena query
        :type query_execution_id: str
        :return: str
        """
        response = self.conn.get_query_execution(QueryExecutionId=query_execution_id)
        state = None
        try:
            state = response['QueryExecution']['Status']['State']
        except Exception as ex:
            self.log.error('Exception while getting query state', ex)
        finally:
            return state

    def get_query_results(self, query_execution_id):
        """
        Fetch submitted athena query results. returns none if query is in intermediate state or
        failed/cancelled state else dict of query output

        :param query_execution_id: Id of submitted athena query
        :type query_execution_id: str
        :return: dict
        """
        query_state = self.check_query_status(query_execution_id)
        if query_state is None:
            self.log.error('Invalid Query state')
            return None
        elif query_state in self.INTERMEDIATE_STATES or query_state in self.FAILURE_STATES:
            self.log.error('Query is in {state} state. Cannot fetch results'.format(state=query_state))
            return None
        return self.conn.get_query_results(QueryExecutionId=query_execution_id)

    def poll_query_status(self, query_execution_id, max_tries=None):
        """
        Poll the status of submitted athena query until query state reaches final state.
        Returns one of the final states

        :param query_execution_id: Id of submitted athena query
        :type query_execution_id: str
        :param max_tries: Number of times to poll for query state before function exits
        :type max_tries: int
        :return: str
        """
        try_number = 1
        final_query_state = None  # Query state when query reaches final state or max_tries reached
        while True:
            query_state = self.check_query_status(query_execution_id)
            if query_state is None:
                self.log.info('Trial {try_number}: Invalid query state. Retrying again'.format(
                    try_number=try_number))
            elif query_state in self.INTERMEDIATE_STATES:
                self.log.info('Trial {try_number}: Query is still in an intermediate state - {state}'
                              .format(try_number=try_number, state=query_state))
            else:
                self.log.info('Trial {try_number}: Query execution completed. Final state is {state}'
                              .format(try_number=try_number, state=query_state))
                final_query_state = query_state
                break
            if max_tries and try_number >= max_tries:  # Break loop if max_tries reached
                final_query_state = query_state
                break
            try_number += 1
            sleep(self.sleep_time)
        return final_query_state

    def stop_query(self, query_execution_id):
        """
        Cancel the submitted athena query

        :param query_execution_id: Id of submitted athena query
        :type query_execution_id: str
        :return: dict
        """
        return self.conn.stop_query_execution(QueryExecutionId=query_execution_id)


class AWSAthenaHelpers(AWSAthenaHook):
    """
    The Athena Helpers contains helper methods to execute queries against
    Athena. The methods can be used directly by operators.
    """

    def __init__(self, aws_conn_id='aws_default', region_name=None, *args, **kwargs):
        super(AWSAthenaHelpers, self).__init__(
            aws_conn_id=aws_conn_id, region_name=region_name, **kwargs)
        self.region_name = region_name
        self.s3_hook = None
        self.glue_hook = None

    def get_s3_hook(self):
        """
        check if s3 hook exists already or create one and return it
        :return: s3 hook
        """
        if not self.s3_hook:
            self.s3_hook = S3Hook(
                aws_conn_id=self.aws_conn_id, verify=self.verify)
        return self.s3_hook

    def get_glue_hook(self):
        """
        check if glue hook exists already or create one and return it
        :return: glue hook
        """
        if not self.glue_hook:
            self.glue_hook = AwsGlueCatalogHook(
                aws_conn_id=self.aws_conn_id, region_name=self.region_name)
        return self.glue_hook

    def run_insert_into_table(self, src_db, src_table, dst_db, dst_table, mode='error'):
        """
        insert data in s3 from the source table to the destination table

        :param src_db: database name of the source table
        :type src_db: str
        :param src_table: the source table
        :type src_table: str
        :param dst_db: database name of the destination table
        :type dst_db: str
        :param dst_table: the destination table
        :type dst_table: str
        :param partitioned_by: the partition key
        :type partitioned_by: list
        :param mode: specifies the behavior when data or table already exists(default: error)
                Options include:
            * `append`: Append contents of this :class:`DataFrame` to existing data.
            * `overwrite`: Overwrite existing data.
            * `error`: Throw an exception if data already exists.
            * `ignore`: Silently ignore this operation if data already exists.
        :type mode: str
        :return: boolean
        """

        modes = ['append', 'overwrite', 'error', 'ignore']
        if mode not in modes:
            raise ValueError('mode should be on of "append", "overwrite", "error" or "ignore"')

        if not self.glue_hook:
            self.get_glue_hook()
        if not self.s3_hook:
            self.get_s3_hook()

        src_location = self.glue_hook.get_table_location(src_db, src_table)
        dst_location = self.glue_hook.get_table_location(dst_db, dst_table)

        src_bucket, src_prefix = S3Hook.parse_s3_url(src_location)
        dst_bucket, dst_prefix = S3Hook.parse_s3_url(dst_location)

        src_keys = self.s3_hook.list_keys(bucket_name=src_bucket, prefix=src_prefix)
        dst_keys = self.s3_hook.list_keys(bucket_name=dst_bucket, prefix=dst_prefix)

        if mode == 'overwrite':
            for key in dst_keys:
                self.s3_hook.delete_objects(dst_bucket, key)

        elif mode == 'error' and len(dst_keys) > 0:
                raise AirflowException('The location of destination table is not empty.')

        elif mode == 'ignore' and len(dst_keys) > 0:
                self.log.info('The location of the destination table is not empty.')

        for key in src_keys:
            suffix = re.sub(src_prefix + '/', '', key)
            src_s3_path = 's3://{}/{}'.format(src_bucket, key)
            dst_s3_path = 's3://{}/{}/{}'.format(dst_bucket, dst_prefix, suffix)
            self.s3_hook.copy_object(src_s3_path, dst_s3_path)

    @staticmethod
    def get_prefix(s3url):
        parsed_url = urlparse(s3url)
        if not parsed_url.netloc:
            raise AirflowException('Please provide a bucket_name instead of "%s"' % s3url)
        else:
            bucket_name = parsed_url.netloc
            key = parsed_url.path.strip('/')
            return bucket_name, key
