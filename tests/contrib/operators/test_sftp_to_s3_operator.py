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

import boto3
from moto import mock_s3

from airflow import configuration
from airflow import models
from airflow.contrib.operators.sftp_to_s3_operator import SFTPToS3Operator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import DAG, TaskInstance
from airflow.settings import Session
from airflow.utils import timezone
from airflow.utils.timezone import datetime
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.hooks.S3_hook import S3Hook


BUCKET = 'test-bucket'
S3_KEY = 'test/test_1_file.csv'
SFTP_PATH = '/tmp/remote_path.txt'
SFTP_CONN_ID = 'ssh_default'
S3_CONN_ID = 'aws_default'

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


class SFTPToS3OperatorTest(unittest.TestCase):

    @mock_s3
    def setUp(self):
        configuration.load_test_config()

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
    def test_sftp_to_s3_operation(self):
        # Setting
        configuration.conf.set("core", "enable_xcom_pickling", "True")
        test_remote_file_content = \
            "This is remote file content \n which is also multiline " \
            "another line here \n this is last line. EOF"

        # create a test file remotely
        create_file_task = SSHOperator(
            task_id="test_create_file",
            ssh_hook=self.hook,
            command="echo '{0}' > {1}".format(test_remote_file_content,
                                              self.sftp_path),
            do_xcom_push=True,
            dag=self.dag
        )
        self.assertIsNotNone(create_file_task)
        ti1 = TaskInstance(task=create_file_task, execution_date=timezone.utcnow())
        ti1.run()

        # Test for creation of s3 bucket
        conn = boto3.client('s3')
        conn.create_bucket(Bucket=self.s3_bucket)
        self.assertTrue((self.s3_hook.check_for_bucket(self.s3_bucket)))

        # get remote file to local
        run_task = SFTPToS3Operator(
            s3_bucket=BUCKET,
            s3_key=S3_KEY,
            sftp_path=SFTP_PATH,
            sftp_conn_id=SFTP_CONN_ID,
            s3_conn_id=S3_CONN_ID,
            task_id='test_sftp_to_s3',
            dag=self.dag
        )
        self.assertIsNotNone(run_task)

        run_task.execute(None)

        # Check if object was created in s3
        objects_in_dest_bucket = conn.list_objects(Bucket=self.s3_bucket,
                                                   Prefix=self.s3_key)
        # there should be object found, and there should only be one object found
        self.assertEqual(len(objects_in_dest_bucket['Contents']), 1)

        # the object found should be consistent with dest_key specified earlier
        self.assertEqual(objects_in_dest_bucket['Contents'][0]['Key'], self.s3_key)

        # Clean up after finishing with test
        conn.delete_object(Bucket=self.s3_bucket, Key=self.s3_key)
        conn.delete_bucket(Bucket=self.s3_bucket)
        self.assertFalse((self.s3_hook.check_for_bucket(self.s3_bucket)))


if __name__ == '__main__':
    unittest.main()
