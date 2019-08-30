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

import unittest

from airflow import configuration
from airflow import models
from airflow.contrib.operators.s3_to_sftp_operator import S3ToSFTPOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import DAG, TaskInstance
from airflow.settings import Session
from airflow.utils import timezone
from airflow.utils.timezone import datetime
import boto3
from moto import mock_s3


TASK_ID = 'test_s3_to_sftp'
BUCKET = 'test-s3-bucket'
S3_KEY = 'test/test_1_file.csv'
SFTP_PATH = '/tmp/remote_path.txt'
SFTP_CONN_ID = 'ssh_default'
S3_CONN_ID = 'aws_default'
LOCAL_FILE_PATH = '/tmp/test_s3_upload'

SFTP_MOCK_FILE = 'test_sftp_file.csv'
S3_MOCK_FILES = 'test_1_file.csv'

TEST_DAG_ID = 'unit_tests'
DEFAULT_DATE = datetime(2018, 1, 1)


def reset(dag_id=TEST_DAG_ID):
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()


reset()


class TestS3ToSFTPOperator(unittest.TestCase):
    @mock_s3
    def setUp(self):
        from airflow.contrib.hooks.ssh_hook import SSHHook
        from airflow.hooks.S3_hook import S3Hook

        hook = SSHHook(ssh_conn_id='ssh_default')
        s3_hook = S3Hook('aws_default')
        hook.no_host_key_check = True
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
            'provide_context': True
        }
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_once', default_args=args)
        dag.schedule_interval = '@once'

        self.hook = hook
        self.s3_hook = s3_hook

        self.ssh_client = self.hook.get_conn()
        self.sftp_client = self.ssh_client.open_sftp()

        self.dag = dag
        self.s3_bucket = BUCKET
        self.sftp_path = SFTP_PATH
        self.s3_key = S3_KEY

    @mock_s3
    def test_s3_to_sftp_operation(self):
        # Setting
        configuration.conf.set("core", "enable_xcom_pickling", "True")
        test_remote_file_content = \
            "This is remote file content \n which is also multiline " \
            "another line here \n this is last line. EOF"

        # Test for creation of s3 bucket
        conn = boto3.client('s3')
        conn.create_bucket(Bucket=self.s3_bucket)
        self.assertTrue(self.s3_hook.check_for_bucket(self.s3_bucket))

        with open(LOCAL_FILE_PATH, 'w') as file:
            file.write(test_remote_file_content)
        self.s3_hook.load_file(LOCAL_FILE_PATH, self.s3_key, bucket_name=BUCKET)

        # Check if object was created in s3
        objects_in_dest_bucket = conn.list_objects(Bucket=self.s3_bucket,
                                                   Prefix=self.s3_key)
        # there should be object found, and there should only be one object found
        self.assertEqual(len(objects_in_dest_bucket['Contents']), 1)

        # the object found should be consistent with dest_key specified earlier
        self.assertEqual(objects_in_dest_bucket['Contents'][0]['Key'], self.s3_key)

        # get remote file to local
        run_task = S3ToSFTPOperator(
            s3_bucket=BUCKET,
            s3_key=S3_KEY,
            sftp_path=SFTP_PATH,
            sftp_conn_id=SFTP_CONN_ID,
            s3_conn_id=S3_CONN_ID,
            task_id=TASK_ID,
            dag=self.dag
        )
        self.assertIsNotNone(run_task)

        run_task.execute(None)

        # Check that the file is created remotely
        check_file_task = SSHOperator(
            task_id="test_check_file",
            ssh_hook=self.hook,
            command="cat {0}".format(self.sftp_path),
            do_xcom_push=True,
            dag=self.dag
        )
        self.assertIsNotNone(check_file_task)
        ti3 = TaskInstance(task=check_file_task, execution_date=timezone.utcnow())
        ti3.run()
        self.assertEqual(
            ti3.xcom_pull(task_ids='test_check_file', key='return_value').strip(),
            test_remote_file_content.encode('utf-8'))

        # Clean up after finishing with test
        conn.delete_object(Bucket=self.s3_bucket, Key=self.s3_key)
        conn.delete_bucket(Bucket=self.s3_bucket)
        self.assertFalse((self.s3_hook.check_for_bucket(self.s3_bucket)))

    def delete_remote_resource(self):
        # check the remote file content
        remove_file_task = SSHOperator(
            task_id="test_check_file",
            ssh_hook=self.hook,
            command="rm {0}".format(self.sftp_path),
            do_xcom_push=True,
            dag=self.dag
        )
        self.assertIsNotNone(remove_file_task)
        ti3 = TaskInstance(task=remove_file_task, execution_date=timezone.utcnow())
        ti3.run()

    def tearDown(self):
        self.delete_remote_resource()


if __name__ == '__main__':
    unittest.main()
