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

"""Example DAG demonstrating the usage of the SFTPOperator"""
import os
from datetime import timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG
from airflow.utils.edgemodifier import Label
from airflow.operators.bash import BashOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator, SFTPOperation
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago


def all_files_exist():
    if os.path.isfile("/tmp/dir_for_remote_transfer/from_remote/txt/file1.txt") and \
        os.path.isfile("/tmp/dir_for_remote_transfer/from_remote/txt/file2.txt") and \
        os.path.isfile("/tmp/dir_for_remote_transfer/from_remote/csv/file3.csv") and \
        os.path.isfile("/tmp/dir_for_remote_transfer/from_remote/csv/file4.csv") and \
        os.path.isfile("/tmp/transfer_file/from_remote/put_files_file1.txt") and \
        os.path.isfile("/tmp/transfer_file/from_remote/put_files_file2.txt") and \
        os.path.isfile("/tmp/transfer_file/from_remote/put_file_file1.txt"):
        return True
    else:
        raise FileExistsError("Failed transfer")


with DAG(
    dag_id="example_sftp_operator",
    schedule_interval="@once",
    start_date=days_ago(1),
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    concurrency=4,
    tags=['example'],
) as dag:
    create_test_files = BashOperator(
        task_id="create_test_files",
        bash_command=f"""
        mkdir /tmp/transfer_file & echo "transfer file from local" >> /tmp/transfer_file/put_file_file1.txt &
            echo "transfer file from local" >> /tmp/transfer_file/put_file_without_full_remote_path_file2.txt &
            echo "transfer file from local" >> /tmp/transfer_file/put_files_file1.txt &
            echo "transfer file from local" >> /tmp/transfer_file/put_files_file2.txt &
        mkdir /tmp/dir_for_remote_transfer &
            touch /tmp/dir_for_remote_transfer/file1.txt &
            touch /tmp/dir_for_remote_transfer/file2.txt &
            touch /tmp/dir_for_remote_transfer/file3.csv &
            touch /tmp/dir_for_remote_transfer/file4.csv &
        ls /tmp/transfer_file & ls /tmp/transfer_file & ls /tmp/dir_for_remote_transfer"""
    )

    # transfer one file to remote server with full file_path
    put_file = SFTPOperator(
        task_id="put_file",
        ssh_conn_id="ssh_default",
        local_filepath="/tmp/transfer_file/put_file_file1.txt",
        remote_filepath="/tmp/transfer_file/remote/put_file_file1.txt",
        operation=SFTPOperation.PUT,
        create_intermediate_dirs=True
    )

    # transfer file without absolute remote file path
    put_file_without_full_remote_path = SFTPOperator(
        task_id="put_file_without_full_remote_path",
        ssh_conn_id="ssh_default",
        local_filepath="/tmp/transfer_file/put_file_without_full_remote_path_file2.txt",
        remote_filepath="/tmp/transfer_file/remote/",
        operation=SFTPOperation.PUT,
        create_intermediate_dirs=True
    )

    # transfer 2 files without absolute remote file path
    put_files = SFTPOperator(
        task_id="put_files",
        ssh_conn_id="ssh_default",
        local_filepath=["/tmp/transfer_file/put_files_file1.txt", "/tmp/transfer_file/put_files_file2.txt"],
        remote_filepath="/tmp/transfer_file/remote/",
        operation=SFTPOperation.PUT,
        create_intermediate_dirs=True
    )

    # transfer all files with match ".*.txt" pattern in directory to remote path
    put_dir_txt_files = SFTPOperator(
        task_id="put_dir_txt_files",
        ssh_conn_id="ssh_default",
        local_folder="/tmp/dir_for_remote_transfer/",
        remote_folder="/tmp/dir_for_remote_transfer/remote/txt/",
        regexp_mask=".*.txt",
        operation=SFTPOperation.PUT,
        create_intermediate_dirs=True
    )

    # transfer all files with match ".*.csv" pattern in directory to remote path
    put_dir_csv_files = SFTPOperator(
        task_id="put_dir_csv_files",
        ssh_conn_id="ssh_default",
        local_folder="/tmp/dir_for_remote_transfer/",
        remote_folder="/tmp/dir_for_remote_transfer/remote/csv/",
        regexp_mask=".*.csv",
        operation=SFTPOperation.PUT,
        create_intermediate_dirs=True
    )

    # drop all local tmp files
    drop_local_test_files = BashOperator(
        task_id="drop_local_test_files",
        bash_command="""rm -rf /tmp/transfer_file/* & rm -rf /tmp/dir_for_remote_transfer & ls /tmp"""
    )

    # get file from remote to local
    get_file = SFTPOperator(
        task_id="get_file",
        ssh_conn_id="ssh_default",
        local_filepath="/tmp/transfer_file/from_remote/put_file_file1.txt",
        remote_filepath="/tmp/transfer_file/remote/put_file_file1.txt",
        operation=SFTPOperation.GET,
        create_intermediate_dirs=True
    )

    # get 2 files from remote dir to local dir
    get_files = SFTPOperator(
        task_id="get_files",
        ssh_conn_id="ssh_default",
        local_filepath="/tmp/transfer_file/from_remote/",
        remote_filepath=["/tmp/transfer_file/remote/put_files_file1.txt",
                         "/tmp/transfer_file/remote/put_files_file2.txt"],
        operation=SFTPOperation.GET,
        create_intermediate_dirs=True
    )

    # get all remote files in dir to local dir
    get_csv_dir = SFTPOperator(
        task_id="get_csv_dir",
        ssh_conn_id="ssh_default",
        local_folder="/tmp/dir_for_remote_transfer/from_remote/csv/",
        remote_folder="/tmp/dir_for_remote_transfer/remote/csv/",
        operation=SFTPOperation.GET,
        create_intermediate_dirs=True
    )

    get_txt_dir = SFTPOperator(
        task_id="get_txt_dir",
        ssh_conn_id="ssh_default",
        local_folder="/tmp/dir_for_remote_transfer/from_remote/txt/",
        remote_folder="/tmp/dir_for_remote_transfer/remote/txt/",
        operation=SFTPOperation.GET,
        create_intermediate_dirs=True
    )

    # drop all remote files
    drop_remote_files = SSHOperator(
        task_id="drop_remote_files",
        ssh_conn_id="ssh_default",
        command="rm -rf /tmp/dir_for_remote_transfer & rm -rf /tmp/transfer_file & ls /tmp"
    )

    check_transfer_file = PythonOperator(
        task_id="check_transfer_file",
        python_callable=all_files_exist
    )

    bad_arguments = SFTPOperator(
        task_id="bad_arguments",
        ssh_conn_id="ssh_default",
        local_folder="/tmp/dir_for_remote_transfer/from_remote/csv/",
        remote_filepath=["/tmp/transfer_file/remote/put_files_file1.txt",
                         "/tmp/transfer_file/remote/put_files_file2.txt"],
        operation=SFTPOperation.GET,
        create_intermediate_dirs=True
    )

    create_test_files >> Label("transfer data to remote server") >> \
    [put_file, put_files, put_file_without_full_remote_path, put_dir_txt_files, put_dir_csv_files] >> \
    drop_local_test_files >> Label("transfer data to local server") >> \
    [get_file, get_files, get_csv_dir, get_txt_dir] >> drop_remote_files >> check_transfer_file
