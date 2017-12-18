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

import os
import unittest
from base64 import b64encode

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
        configuration.set("core", "enable_xcom_pickling", "True")
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
        configuration.set("core", "enable_xcom_pickling", "False")
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
        configuration.set("core", "enable_xcom_pickling", "True")
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
        configuration.set("core", "enable_xcom_pickling", "False")
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
