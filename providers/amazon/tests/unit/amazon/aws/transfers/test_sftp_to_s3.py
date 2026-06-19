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
from __future__ import annotations

import warnings

import boto3
import pytest
from moto import mock_aws

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.timezone import datetime

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

BUCKET = "test-bucket"
S3_KEY = "test/test_1_file.csv"
SFTP_PATH = "/tmp/remote_path.txt"
SFTP_CONN_ID = "ssh_default"
S3_CONN_ID = "aws_default"

SFTP_MOCK_FILE = "test_sftp_file.csv"
S3_MOCK_FILES = "test_1_file.csv"

TEST_DAG_ID = "unit_tests_sftp_tos3_op"
DEFAULT_DATE = datetime(2018, 1, 1)


class TestSFTPToS3Operator:
    def setup_method(self):
        hook = SSHHook(ssh_conn_id="ssh_default")

        hook.no_host_key_check = True
        dag = DAG(
            f"{TEST_DAG_ID}test_schedule_dag_once",
            schedule="@once",
            start_date=DEFAULT_DATE,
        )

        self.hook = hook

        self.ssh_client = self.hook.get_conn()
        self.sftp_client = self.ssh_client.open_sftp()

        self.dag = dag
        self.s3_bucket = BUCKET
        self.sftp_path = SFTP_PATH
        self.s3_key = S3_KEY

    @pytest.mark.parametrize("use_temp_file", [True, False])
    @mock_aws
    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_sftp_to_s3_operation(self, use_temp_file):
        # Setting
        test_remote_file_content = (
            "This is remote file content \n which is also multiline "
            "another line here \n this is last line. EOF"
        )

        # create a test file remotely
        create_file_task = SSHOperator(
            task_id="test_create_file",
            ssh_hook=self.hook,
            command=f"echo '{test_remote_file_content}' > {self.sftp_path}",
            do_xcom_push=True,
            dag=self.dag,
        )
        assert create_file_task is not None
        create_file_task.execute(None)

        # Test for creation of s3 bucket
        s3_hook = S3Hook(aws_conn_id=None)
        conn = boto3.client("s3")
        conn.create_bucket(Bucket=self.s3_bucket)
        assert s3_hook.check_for_bucket(self.s3_bucket)

        # get remote file to local
        run_task = SFTPToS3Operator(
            s3_bucket=BUCKET,
            s3_key=S3_KEY,
            sftp_path=SFTP_PATH,
            sftp_conn_id=SFTP_CONN_ID,
            aws_conn_id=S3_CONN_ID,
            use_temp_file=use_temp_file,
            task_id="test_sftp_to_s3",
            dag=self.dag,
        )
        assert run_task is not None

        run_task.execute(None)

        # Check if object was created in s3
        objects_in_dest_bucket = conn.list_objects(Bucket=self.s3_bucket, Prefix=self.s3_key)
        # there should be object found, and there should only be one object found
        assert len(objects_in_dest_bucket["Contents"]) == 1

        # the object found should be consistent with dest_key specified earlier
        assert objects_in_dest_bucket["Contents"][0]["Key"] == self.s3_key

        # Clean up after finishing with test
        conn.delete_object(Bucket=self.s3_bucket, Key=self.s3_key)
        conn.delete_bucket(Bucket=self.s3_bucket)
        assert not s3_hook.check_for_bucket(self.s3_bucket)

    @pytest.mark.parametrize("fail_on_file_not_exist", [True, False])
    @mock_aws
    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_sftp_to_s3_fail_on_file_not_exist(self, fail_on_file_not_exist):
        s3_hook = S3Hook(aws_conn_id=None)
        conn = boto3.client("s3")
        conn.create_bucket(Bucket=self.s3_bucket)
        assert s3_hook.check_for_bucket(self.s3_bucket)

        if fail_on_file_not_exist:
            with pytest.raises(FileNotFoundError):
                SFTPToS3Operator(
                    s3_bucket=self.s3_bucket,
                    s3_key=self.s3_key,
                    sftp_path="/tmp/wrong_path.txt",
                    sftp_conn_id=SFTP_CONN_ID,
                    aws_conn_id=S3_CONN_ID,
                    fail_on_file_not_exist=fail_on_file_not_exist,
                    task_id="test_sftp_to_s3",
                    dag=self.dag,
                ).execute(None)
        else:
            SFTPToS3Operator(
                s3_bucket=self.s3_bucket,
                s3_key=self.s3_key,
                sftp_path=self.sftp_path,
                sftp_conn_id=SFTP_CONN_ID,
                aws_conn_id=S3_CONN_ID,
                fail_on_file_not_exist=fail_on_file_not_exist,
                task_id="test_sftp_to_s3",
                dag=self.dag,
            ).execute(None)

        conn.delete_object(Bucket=self.s3_bucket, Key=self.s3_key)
        conn.delete_bucket(Bucket=self.s3_bucket)
        assert not s3_hook.check_for_bucket(self.s3_bucket)

    @mock_aws
    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_sftp_to_s3_sftp_remote_host(self):
        """Test that sftp_remote_host overrides the connection host when provided."""
        test_remote_file_content = (
            "This is remote file content for sftp_remote_host test \n which is also multiline "
            "another line here \n this is last line. EOF"
        )

        # Create a test file remotely
        create_file_task = SSHOperator(
            task_id="test_create_file_remote_host",
            ssh_hook=self.hook,
            command=f"echo '{test_remote_file_content}' > {self.sftp_path}",
            do_xcom_push=True,
            dag=self.dag,
        )
        assert create_file_task is not None
        create_file_task.execute(None)

        # Test for creation of s3 bucket
        s3_hook = S3Hook(aws_conn_id=None)
        conn = boto3.client("s3")
        conn.create_bucket(Bucket=self.s3_bucket)
        assert s3_hook.check_for_bucket(self.s3_bucket)

        # Execute with sftp_remote_host overriding the connection host to the same localhost
        run_task = SFTPToS3Operator(
            s3_bucket=BUCKET,
            s3_key=S3_KEY,
            sftp_path=SFTP_PATH,
            sftp_conn_id=SFTP_CONN_ID,
            sftp_remote_host="localhost",
            aws_conn_id=S3_CONN_ID,
            task_id="test_sftp_to_s3_remote_host",
            dag=self.dag,
        )
        assert run_task is not None

        run_task.execute(None)

        # Check if object was created in s3
        objects_in_dest_bucket = conn.list_objects(Bucket=self.s3_bucket, Prefix=self.s3_key)
        assert len(objects_in_dest_bucket["Contents"]) == 1
        assert objects_in_dest_bucket["Contents"][0]["Key"] == self.s3_key

        # Clean up after finishing with test
        conn.delete_object(Bucket=self.s3_bucket, Key=self.s3_key)
        conn.delete_bucket(Bucket=self.s3_bucket)
        assert not s3_hook.check_for_bucket(self.s3_bucket)


class TestSFTPToS3OperatorInit:
    """Unit tests for SFTPToS3Operator.__init__ that do not require an SSH server."""

    def test_s3_conn_id_deprecated(self):
        """s3_conn_id is a deprecated alias for aws_conn_id and must raise DeprecationWarning."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            op = SFTPToS3Operator(
                task_id="test_deprecated",
                s3_bucket=BUCKET,
                s3_key=S3_KEY,
                sftp_path=SFTP_PATH,
                sftp_conn_id=SFTP_CONN_ID,
                s3_conn_id="my_legacy_conn",
            )
        deprecation_warnings = [
            w for w in caught if issubclass(w.category, AirflowProviderDeprecationWarning)
        ]
        assert len(deprecation_warnings) == 1
        assert "s3_conn_id" in str(deprecation_warnings[0].message)
        assert op.aws_conn_id == "my_legacy_conn"

    def test_aws_conn_id_default(self):
        """aws_conn_id defaults to 'aws_default' and no AirflowProviderDeprecationWarning is raised."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            op = SFTPToS3Operator(
                task_id="test_default",
                s3_bucket=BUCKET,
                s3_key=S3_KEY,
                sftp_path=SFTP_PATH,
                sftp_conn_id=SFTP_CONN_ID,
            )
        deprecation_warnings = [
            w for w in caught if issubclass(w.category, AirflowProviderDeprecationWarning)
        ]
        assert not deprecation_warnings
        assert op.aws_conn_id == "aws_default"

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({}, {"replace": False, "encrypt": False, "gzip": False, "acl_policy": None}),
            (
                {"replace": True, "encrypt": True, "gzip": True, "acl_policy": "bucket-owner-full-control"},
                {
                    "replace": True,
                    "encrypt": True,
                    "gzip": True,
                    "acl_policy": "bucket-owner-full-control",
                },
            ),
        ],
    )
    def test_s3_upload_options(self, kwargs, expected):
        """replace/encrypt/gzip/acl_policy are stored and default to False/None."""
        op = SFTPToS3Operator(
            task_id="test_options",
            s3_bucket=BUCKET,
            s3_key=S3_KEY,
            sftp_path=SFTP_PATH,
            sftp_conn_id=SFTP_CONN_ID,
            **kwargs,
        )
        assert op.replace == expected["replace"]
        assert op.encrypt == expected["encrypt"]
        assert op.gzip == expected["gzip"]
        assert op.acl_policy == expected["acl_policy"]

    @pytest.mark.parametrize(
        ("sftp_filenames", "s3_filenames"),
        [
            (None, None),
            ("*", None),
            ("prefix_", "renamed_"),
            (["a.csv", "b.csv"], ["x.csv", "y.csv"]),
        ],
    )
    def test_multi_file_params(self, sftp_filenames, s3_filenames):
        """sftp_filenames and s3_filenames are stored correctly."""
        op = SFTPToS3Operator(
            task_id="test_multi",
            s3_bucket=BUCKET,
            s3_key=S3_KEY,
            sftp_path=SFTP_PATH,
            sftp_conn_id=SFTP_CONN_ID,
            sftp_filenames=sftp_filenames,
            s3_filenames=s3_filenames,
        )
        assert op.sftp_filenames == sftp_filenames
        assert op.s3_filenames == s3_filenames
