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

import os
import unittest
from base64 import b64encode
import six

from airflow import configuration
from airflow import models
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import DAG, TaskInstance
from airflow.settings import Session
from airflow.utils import timezone
from airflow.utils.timezone import datetime

TEST_DAG_ID = 'unit_tests'
DEFAULT_DATE = datetime(2017, 1, 1)


def reset(dag_id=TEST_DAG_ID):
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()

reset()


class SFTPOperatorTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        from airflow.contrib.hooks.ssh_hook import SSHHook
        hook = SSHHook(ssh_conn_id='ssh_default')
        hook.no_host_key_check = True
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
            'provide_context': True
        }
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_once', default_args=args)
        dag.schedule_interval = '@once'
        self.hook = hook
        self.dag = dag
        self.test_dir = "/tmp"
        self.test_local_filename = 'test_local_file'
        self.test_remote_filename = 'test_remote_file'
        self.test_local_filepath = '{0}/{1}'.format(self.test_dir,
                                                    self.test_local_filename)
        self.test_remote_filepath = '{0}/{1}'.format(self.test_dir,
                                                     self.test_remote_filename)

    def test_pickle_file_transfer_put(self):
        configuration.conf.set("core", "enable_xcom_pickling", "True")
        test_local_file_content = \
            b"This is local file content \n which is multiline " \
            b"continuing....with other character\nanother line here \n this is last line"
        # create a test file locally
        with open(self.test_local_filepath, 'wb') as f:
            f.write(test_local_file_content)

        # put test file to remote
        put_test_task = SFTPOperator(
                task_id="test_sftp",
                ssh_hook=self.hook,
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.PUT,
                dag=self.dag
        )
        self.assertIsNotNone(put_test_task)
        ti2 = TaskInstance(task=put_test_task, execution_date=timezone.utcnow())
        ti2.run()

        # check the remote file content
        check_file_task = SSHOperator(
                task_id="test_check_file",
                ssh_hook=self.hook,
                command="cat {0}".format(self.test_remote_filepath),
                do_xcom_push=True,
                dag=self.dag
        )
        self.assertIsNotNone(check_file_task)
        ti3 = TaskInstance(task=check_file_task, execution_date=timezone.utcnow())
        ti3.run()
        self.assertEqual(
                ti3.xcom_pull(task_ids='test_check_file', key='return_value').strip(),
                test_local_file_content)

    def test_json_file_transfer_put(self):
        configuration.conf.set("core", "enable_xcom_pickling", "False")
        test_local_file_content = \
            b"This is local file content \n which is multiline " \
            b"continuing....with other character\nanother line here \n this is last line"
        # create a test file locally
        with open(self.test_local_filepath, 'wb') as f:
            f.write(test_local_file_content)

        # put test file to remote
        put_test_task = SFTPOperator(
                task_id="test_sftp",
                ssh_hook=self.hook,
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.PUT,
                dag=self.dag
        )
        self.assertIsNotNone(put_test_task)
        ti2 = TaskInstance(task=put_test_task, execution_date=timezone.utcnow())
        ti2.run()

        # check the remote file content
        check_file_task = SSHOperator(
                task_id="test_check_file",
                ssh_hook=self.hook,
                command="cat {0}".format(self.test_remote_filepath),
                do_xcom_push=True,
                dag=self.dag
        )
        self.assertIsNotNone(check_file_task)
        ti3 = TaskInstance(task=check_file_task, execution_date=timezone.utcnow())
        ti3.run()
        self.assertEqual(
                ti3.xcom_pull(task_ids='test_check_file', key='return_value').strip(),
                b64encode(test_local_file_content).decode('utf-8'))


    def test_pickle_file_transfer_get(self):
        configuration.conf.set("core", "enable_xcom_pickling", "True")
        test_remote_file_content = \
            "This is remote file content \n which is also multiline " \
            "another line here \n this is last line. EOF"

        # create a test file remotely
        create_file_task = SSHOperator(
                task_id="test_create_file",
                ssh_hook=self.hook,
                command="echo '{0}' > {1}".format(test_remote_file_content,
                                                  self.test_remote_filepath),
                do_xcom_push=True,
                dag=self.dag
        )
        self.assertIsNotNone(create_file_task)
        ti1 = TaskInstance(task=create_file_task, execution_date=timezone.utcnow())
        ti1.run()

        # get remote file to local
        get_test_task = SFTPOperator(
                task_id="test_sftp",
                ssh_hook=self.hook,
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.GET,
                dag=self.dag
        )
        self.assertIsNotNone(get_test_task)
        ti2 = TaskInstance(task=get_test_task, execution_date=timezone.utcnow())
        ti2.run()

        # test the received content
        content_received = None
        with open(self.test_local_filepath, 'r') as f:
            content_received = f.read()
        self.assertEqual(content_received.strip(), test_remote_file_content)

    def test_json_file_transfer_get(self):
        configuration.conf.set("core", "enable_xcom_pickling", "False")
        test_remote_file_content = \
            "This is remote file content \n which is also multiline " \
            "another line here \n this is last line. EOF"

        # create a test file remotely
        create_file_task = SSHOperator(
                task_id="test_create_file",
                ssh_hook=self.hook,
                command="echo '{0}' > {1}".format(test_remote_file_content,
                                                  self.test_remote_filepath),
                do_xcom_push=True,
                dag=self.dag
        )
        self.assertIsNotNone(create_file_task)
        ti1 = TaskInstance(task=create_file_task, execution_date=timezone.utcnow())
        ti1.run()

        # get remote file to local
        get_test_task = SFTPOperator(
                task_id="test_sftp",
                ssh_hook=self.hook,
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.GET,
                dag=self.dag
        )
        self.assertIsNotNone(get_test_task)
        ti2 = TaskInstance(task=get_test_task, execution_date=timezone.utcnow())
        ti2.run()

        # test the received content
        content_received = None
        with open(self.test_local_filepath, 'r') as f:
            content_received = f.read()
        self.assertEqual(content_received.strip(),
            test_remote_file_content.encode('utf-8').decode('utf-8'))

    def test_arg_checking(self):
        from airflow.exceptions import AirflowException
        conn_id = "conn_id_for_testing"
        os.environ['AIRFLOW_CONN_' + conn_id.upper()] = "ssh://test_id@localhost"

        # Exception should be raised if neither ssh_hook nor ssh_conn_id is provided
        if six.PY2:
            self.assertRaisesRegex = self.assertRaisesRegexp
        with self.assertRaisesRegex(AirflowException,
                                    "Cannot operate without ssh_hook or ssh_conn_id."):
            task_0 = SFTPOperator(
                task_id="test_sftp",
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.PUT,
                dag=self.dag
            )
            task_0.execute(None)

        # if ssh_hook is invalid/not provided, use ssh_conn_id to create SSHHook
        task_1 = SFTPOperator(
            task_id="test_sftp",
            ssh_hook="string_rather_than_SSHHook",  # invalid ssh_hook
            ssh_conn_id=conn_id,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=SFTPOperation.PUT,
            dag=self.dag
        )
        try:
            task_1.execute(None)
        except Exception:
            pass
        self.assertEqual(task_1.ssh_hook.ssh_conn_id, conn_id)

        task_2 = SFTPOperator(
            task_id="test_sftp",
            ssh_conn_id=conn_id,  # no ssh_hook provided
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=SFTPOperation.PUT,
            dag=self.dag
        )
        try:
            task_2.execute(None)
        except Exception:
            pass
        self.assertEqual(task_2.ssh_hook.ssh_conn_id, conn_id)

        # if both valid ssh_hook and ssh_conn_id are provided, ignore ssh_conn_id
        task_3 = SFTPOperator(
            task_id="test_sftp",
            ssh_hook=self.hook,
            ssh_conn_id=conn_id,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=SFTPOperation.PUT,
            dag=self.dag
        )
        try:
            task_3.execute(None)
        except Exception:
            pass
        self.assertEqual(task_3.ssh_hook.ssh_conn_id, self.hook.ssh_conn_id)

    def delete_local_resource(self):
        if os.path.exists(self.test_local_filepath):
            os.remove(self.test_local_filepath)

    def delete_remote_resource(self):
        # check the remote file content
        remove_file_task = SSHOperator(
            task_id="test_check_file",
            ssh_hook=self.hook,
            command="rm {0}".format(self.test_remote_filepath),
            do_xcom_push=True,
            dag=self.dag
        )
        self.assertIsNotNone(remove_file_task)
        ti3 = TaskInstance(task=remove_file_task, execution_date=timezone.utcnow())
        ti3.run()

    def tearDown(self):
        self.delete_local_resource() and self.delete_remote_resource()


if __name__ == '__main__':
    unittest.main()
