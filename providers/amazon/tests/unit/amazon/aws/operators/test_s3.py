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

import errno
import os
import shutil
import sys
from datetime import timedelta
from io import BytesIO
from tempfile import mkdtemp
from unittest import mock

import boto3
import pytest
from moto import mock_aws

from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import (
    S3CopyObjectOperator,
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
    S3DeleteBucketTaggingOperator,
    S3DeleteObjectsOperator,
    S3FileTransformOperator,
    S3GetBucketTaggingOperator,
    S3ListOperator,
    S3ListPrefixesOperator,
    S3PutBucketTaggingOperator,
)
from airflow.providers.common.compat.openlineage.facet import (
    Dataset,
    LifecycleStateChange,
    LifecycleStateChangeDatasetFacet,
    PreviousIdentifier,
)
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.openlineage.extractors import OperatorLineage
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS
from unit.amazon.aws.utils.test_template_fields import validate_template_fields

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk.timezone import datetime, utcnow
else:
    from airflow.utils.timezone import datetime, utcnow  # type: ignore[attr-defined,no-redef]

BUCKET_NAME = os.environ.get("BUCKET_NAME", "test-airflow-bucket")
S3_KEY = "test-airflow-key"
TAG_SET = [{"Key": "Color", "Value": "Green"}]


class TestS3CreateBucketOperator:
    def setup_method(self):
        self.create_bucket_operator = S3CreateBucketOperator(
            task_id="test-s3-create-bucket-operator",
            bucket_name=BUCKET_NAME,
        )

    @mock_aws
    @mock.patch.object(S3Hook, "create_bucket")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_bucket_exist(self, mock_check_for_bucket, mock_create_bucket):
        mock_check_for_bucket.return_value = True
        # execute s3 bucket create operator
        self.create_bucket_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        mock_create_bucket.assert_not_called()

    @mock_aws
    @mock.patch.object(S3Hook, "create_bucket")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_not_bucket_exist(self, mock_check_for_bucket, mock_create_bucket):
        mock_check_for_bucket.return_value = False
        # execute s3 bucket create operator
        self.create_bucket_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        mock_create_bucket.assert_called_once_with(bucket_name=BUCKET_NAME, region_name=None)

    def test_template_fields(self):
        validate_template_fields(self.create_bucket_operator)


class TestS3DeleteBucketOperator:
    def setup_method(self):
        self.delete_bucket_operator = S3DeleteBucketOperator(
            task_id="test-s3-delete-operator",
            bucket_name=BUCKET_NAME,
        )

    @mock_aws
    @mock.patch.object(S3Hook, "delete_bucket")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_bucket_exist(self, mock_check_for_bucket, mock_delete_bucket):
        mock_check_for_bucket.return_value = True
        # execute s3 bucket delete operator
        self.delete_bucket_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        mock_delete_bucket.assert_called_once_with(bucket_name=BUCKET_NAME, force_delete=False)

    @mock_aws
    @mock.patch.object(S3Hook, "delete_bucket")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_not_bucket_exist(self, mock_check_for_bucket, mock_delete_bucket):
        mock_check_for_bucket.return_value = False
        # execute s3 bucket delete operator
        self.delete_bucket_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        mock_delete_bucket.assert_not_called()

    def test_template_fields(self):
        validate_template_fields(self.delete_bucket_operator)


class TestS3GetBucketTaggingOperator:
    def setup_method(self):
        self.get_bucket_tagging_operator = S3GetBucketTaggingOperator(
            task_id="test-s3-get-bucket-tagging-operator",
            bucket_name=BUCKET_NAME,
        )

    @mock_aws
    @mock.patch.object(S3Hook, "get_bucket_tagging")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_bucket_exist(self, mock_check_for_bucket, get_bucket_tagging):
        mock_check_for_bucket.return_value = True
        # execute s3 get bucket tagging operator
        self.get_bucket_tagging_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        get_bucket_tagging.assert_called_once_with(BUCKET_NAME)

    @mock_aws
    @mock.patch.object(S3Hook, "get_bucket_tagging")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_not_bucket_exist(self, mock_check_for_bucket, get_bucket_tagging):
        mock_check_for_bucket.return_value = False
        # execute s3 get bucket tagging operator
        self.get_bucket_tagging_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        get_bucket_tagging.assert_not_called()

    def test_template_fields(self):
        validate_template_fields(self.get_bucket_tagging_operator)


class TestS3PutBucketTaggingOperator:
    def setup_method(self):
        self.put_bucket_tagging_operator = S3PutBucketTaggingOperator(
            task_id="test-s3-put-bucket-tagging-operator",
            tag_set=TAG_SET,
            bucket_name=BUCKET_NAME,
        )

    @mock_aws
    @mock.patch.object(S3Hook, "put_bucket_tagging")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_bucket_exist(self, mock_check_for_bucket, put_bucket_tagging):
        mock_check_for_bucket.return_value = True
        # execute s3 put bucket tagging operator
        self.put_bucket_tagging_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        put_bucket_tagging.assert_called_once_with(
            key=None, value=None, tag_set=TAG_SET, bucket_name=BUCKET_NAME
        )

    @mock_aws
    @mock.patch.object(S3Hook, "put_bucket_tagging")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_not_bucket_exist(self, mock_check_for_bucket, put_bucket_tagging):
        mock_check_for_bucket.return_value = False
        # execute s3 put bucket tagging operator
        self.put_bucket_tagging_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        put_bucket_tagging.assert_not_called()

    def test_template_fields(self):
        validate_template_fields(self.put_bucket_tagging_operator)


class TestS3DeleteBucketTaggingOperator:
    def setup_method(self):
        self.delete_bucket_tagging_operator = S3DeleteBucketTaggingOperator(
            task_id="test-s3-delete-bucket-tagging-operator",
            bucket_name=BUCKET_NAME,
        )

    @mock_aws
    @mock.patch.object(S3Hook, "delete_bucket_tagging")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_bucket_exist(self, mock_check_for_bucket, delete_bucket_tagging):
        mock_check_for_bucket.return_value = True
        # execute s3 delete bucket tagging operator
        self.delete_bucket_tagging_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        delete_bucket_tagging.assert_called_once_with(BUCKET_NAME)

    @mock_aws
    @mock.patch.object(S3Hook, "delete_bucket_tagging")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_not_bucket_exist(self, mock_check_for_bucket, delete_bucket_tagging):
        mock_check_for_bucket.return_value = False
        # execute s3 delete bucket tagging operator
        self.delete_bucket_tagging_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        delete_bucket_tagging.assert_not_called()

    def test_template_fields(self):
        validate_template_fields(self.delete_bucket_tagging_operator)


class TestS3FileTransformOperator:
    def setup_method(self):
        self.content = b"input"
        self.bucket = "bucket"
        self.input_key = "foo"
        self.output_key = "bar"
        self.bio = BytesIO(self.content)
        self.tmp_dir = mkdtemp(prefix="test_tmpS3FileTransform_")
        self.transform_script = os.path.join(self.tmp_dir, "transform.py")
        os.mknod(self.transform_script)

    def teardown_method(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError as e:
            # ENOENT - no such file or directory
            if e.errno != errno.ENOENT:
                raise e

    @mock.patch("subprocess.Popen")
    @mock.patch.object(S3FileTransformOperator, "log")
    @mock_aws
    def test_execute_with_transform_script(self, mock_log, mock_popen):
        process_output = [b"Foo", b"Bar", b"Baz"]
        self.mock_process(mock_popen, process_output=process_output)
        input_path, output_path = self.s3_paths()

        op = S3FileTransformOperator(
            source_s3_key=input_path,
            dest_s3_key=output_path,
            transform_script=self.transform_script,
            replace=True,
            task_id="task_id",
        )
        op.execute(None)

        mock_log.info.assert_has_calls(
            [mock.call(line.decode(sys.getdefaultencoding())) for line in process_output]
        )

    @mock.patch("subprocess.Popen")
    @mock_aws
    def test_execute_with_failing_transform_script(self, mock_popen):
        self.mock_process(mock_popen, return_code=42)
        input_path, output_path = self.s3_paths()

        op = S3FileTransformOperator(
            source_s3_key=input_path,
            dest_s3_key=output_path,
            transform_script=self.transform_script,
            replace=True,
            task_id="task_id",
        )

        with pytest.raises(AirflowException) as ctx:
            op.execute(None)

        assert str(ctx.value) == "Transform script failed: 42"

    @mock.patch("subprocess.Popen")
    @mock_aws
    def test_execute_with_transform_script_args(self, mock_popen):
        self.mock_process(mock_popen, process_output=[b"Foo", b"Bar", b"Baz"])
        input_path, output_path = self.s3_paths()
        script_args = ["arg1", "arg2"]

        op = S3FileTransformOperator(
            source_s3_key=input_path,
            dest_s3_key=output_path,
            transform_script=self.transform_script,
            script_args=script_args,
            replace=True,
            task_id="task_id",
        )
        op.execute(None)

        assert script_args == mock_popen.call_args.args[0][3:]

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.select_key", return_value="input")
    @mock_aws
    def test_execute_with_select_expression(self, mock_select_key):
        input_path, output_path = self.s3_paths()
        select_expression = "SELECT * FROM s3object s"
        input_serialization = None
        output_serialization = None

        op = S3FileTransformOperator(
            source_s3_key=input_path,
            dest_s3_key=output_path,
            select_expression=select_expression,
            replace=True,
            task_id="task_id",
        )
        op.execute(None)

        mock_select_key.assert_called_once_with(
            key=input_path,
            expression=select_expression,
            input_serialization=input_serialization,
            output_serialization=output_serialization,
        )

        conn = boto3.client("s3")
        result = conn.get_object(Bucket=self.bucket, Key=self.output_key)
        assert self.content == result["Body"].read()

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.select_key", return_value="input")
    @mock_aws
    def test_execute_with_select_expression_and_serialization_config(self, mock_select_key):
        input_path, output_path = self.s3_paths()
        select_expression = "SELECT * FROM s3object s"
        select_expr_serialization_config = {
            "input_serialization": {"CSV": {}},
            "output_serialization": {"CSV": {}},
        }

        op = S3FileTransformOperator(
            source_s3_key=input_path,
            dest_s3_key=output_path,
            select_expression=select_expression,
            select_expr_serialization_config=select_expr_serialization_config,
            replace=True,
            task_id="task_id",
        )
        op.execute(None)

        input_serialization = select_expr_serialization_config.get("input_serialization")
        output_serialization = select_expr_serialization_config.get("output_serialization")
        mock_select_key.assert_called_once_with(
            key=input_path,
            expression=select_expression,
            input_serialization=input_serialization,
            output_serialization=output_serialization,
        )

        conn = boto3.client("s3")
        result = conn.get_object(Bucket=self.bucket, Key=self.output_key)
        assert self.content == result["Body"].read()

    def test_get_openlineage_facets_on_start(self):
        expected_input = Dataset(
            namespace=f"s3://{self.bucket}",
            name=self.input_key,
        )
        expected_output = Dataset(
            namespace=f"s3://{self.bucket}",
            name=self.output_key,
        )

        op = S3FileTransformOperator(
            task_id="test",
            source_s3_key=f"s3://{self.bucket}/{self.input_key}",
            dest_s3_key=f"s3://{self.bucket}/{self.output_key}",
        )

        lineage = op.get_openlineage_facets_on_start()
        assert len(lineage.inputs) == 1
        assert len(lineage.outputs) == 1
        assert lineage.inputs[0] == expected_input
        assert lineage.outputs[0] == expected_output

    @staticmethod
    def mock_process(mock_popen, return_code=0, process_output=None):
        mock_proc = mock.MagicMock()
        mock_proc.returncode = return_code
        mock_proc.stdout.readline.side_effect = process_output or []
        mock_proc.wait.return_value = None
        mock_popen.return_value.__enter__.return_value = mock_proc

    def s3_paths(self):
        conn = boto3.client("s3")
        conn.create_bucket(Bucket=self.bucket)
        conn.upload_fileobj(Bucket=self.bucket, Key=self.input_key, Fileobj=self.bio)

        s3_url = "s3://{0}/{1}"
        input_path = s3_url.format(self.bucket, self.input_key)
        output_path = s3_url.format(self.bucket, self.output_key)

        return input_path, output_path

    def test_template_fields(self):
        operator = S3FileTransformOperator(
            source_s3_key="test/key",
            dest_s3_key="test/key",
            transform_script=self.transform_script,
            replace=True,
            task_id="task_id",
        )
        validate_template_fields(operator)


class TestS3ListOperator:
    def test_execute(self):
        operator = S3ListOperator(
            task_id="test-s3-list-operator",
            bucket=BUCKET_NAME,
            prefix="TEST",
            delimiter=".csv",
        )
        operator.hook = mock.MagicMock()
        operator.hook.list_keys.return_value = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]

        files = operator.execute(None)

        operator.hook.list_keys.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            prefix="TEST",
            delimiter=".csv",
            apply_wildcard=False,
        )
        assert sorted(files) == sorted(["TEST1.csv", "TEST2.csv", "TEST3.csv"])

    def test_template_fields(self):
        operator = S3ListOperator(
            task_id="test-s3-list-operator",
            bucket=BUCKET_NAME,
            prefix="TEST",
            delimiter=".csv",
        )
        validate_template_fields(operator)


class TestS3ListPrefixesOperator:
    def test_execute(self):
        operator = S3ListPrefixesOperator(
            task_id="test-s3-list-prefixes-operator", bucket=BUCKET_NAME, prefix="test/", delimiter="/"
        )
        operator.hook = mock.MagicMock()
        operator.hook.list_prefixes.return_value = ["test/"]

        subfolders = operator.execute(None)

        operator.hook.list_prefixes.assert_called_once_with(
            bucket_name=BUCKET_NAME, prefix="test/", delimiter="/"
        )
        assert subfolders == ["test/"]

    def test_template_fields(self):
        operator = S3ListPrefixesOperator(
            task_id="test-s3-list-prefixes-operator", bucket=BUCKET_NAME, prefix="test/", delimiter="/"
        )
        validate_template_fields(operator)


class TestS3CopyObjectOperator:
    def setup_method(self):
        self.source_bucket = "bucket1"
        self.source_key = "path1/data.txt"
        self.dest_bucket = "bucket2"
        self.dest_key = "path2/data_copy.txt"

    @mock_aws
    def test_s3_copy_object_arg_combination_1(self):
        conn = boto3.client("s3")
        conn.create_bucket(Bucket=self.source_bucket)
        conn.create_bucket(Bucket=self.dest_bucket)
        conn.upload_fileobj(Bucket=self.source_bucket, Key=self.source_key, Fileobj=BytesIO(b"input"))

        # there should be nothing found before S3CopyObjectOperator is executed
        assert "Contents" not in conn.list_objects(Bucket=self.dest_bucket, Prefix=self.dest_key)

        op = S3CopyObjectOperator(
            task_id="test_task_s3_copy_object",
            source_bucket_key=self.source_key,
            source_bucket_name=self.source_bucket,
            dest_bucket_key=self.dest_key,
            dest_bucket_name=self.dest_bucket,
        )
        op.execute(None)

        objects_in_dest_bucket = conn.list_objects(Bucket=self.dest_bucket, Prefix=self.dest_key)
        # there should be object found, and there should only be one object found
        assert len(objects_in_dest_bucket["Contents"]) == 1
        # the object found should be consistent with dest_key specified earlier
        assert objects_in_dest_bucket["Contents"][0]["Key"] == self.dest_key

    @mock_aws
    def test_s3_copy_object_arg_combination_2(self):
        conn = boto3.client("s3")
        conn.create_bucket(Bucket=self.source_bucket)
        conn.create_bucket(Bucket=self.dest_bucket)
        conn.upload_fileobj(Bucket=self.source_bucket, Key=self.source_key, Fileobj=BytesIO(b"input"))

        # there should be nothing found before S3CopyObjectOperator is executed
        assert "Contents" not in conn.list_objects(Bucket=self.dest_bucket, Prefix=self.dest_key)

        source_key_s3_url = f"s3://{self.source_bucket}/{self.source_key}"
        dest_key_s3_url = f"s3://{self.dest_bucket}/{self.dest_key}"
        op = S3CopyObjectOperator(
            task_id="test_task_s3_copy_object",
            source_bucket_key=source_key_s3_url,
            dest_bucket_key=dest_key_s3_url,
        )
        op.execute(None)

        objects_in_dest_bucket = conn.list_objects(Bucket=self.dest_bucket, Prefix=self.dest_key)
        # there should be object found, and there should only be one object found
        assert len(objects_in_dest_bucket["Contents"]) == 1
        # the object found should be consistent with dest_key specified earlier
        assert objects_in_dest_bucket["Contents"][0]["Key"] == self.dest_key

    def test_get_openlineage_facets_on_start_combination_1(self):
        expected_input = Dataset(
            namespace=f"s3://{self.source_bucket}",
            name=self.source_key,
        )
        expected_output = Dataset(
            namespace=f"s3://{self.dest_bucket}",
            name=self.dest_key,
        )

        op = S3CopyObjectOperator(
            task_id="test",
            source_bucket_name=self.source_bucket,
            source_bucket_key=self.source_key,
            dest_bucket_name=self.dest_bucket,
            dest_bucket_key=self.dest_key,
        )

        lineage = op.get_openlineage_facets_on_start()
        assert len(lineage.inputs) == 1
        assert len(lineage.outputs) == 1
        assert lineage.inputs[0] == expected_input
        assert lineage.outputs[0] == expected_output

    def test_get_openlineage_facets_on_start_combination_2(self):
        expected_input = Dataset(
            namespace=f"s3://{self.source_bucket}",
            name=self.source_key,
        )
        expected_output = Dataset(
            namespace=f"s3://{self.dest_bucket}",
            name=self.dest_key,
        )

        source_key_s3_url = f"s3://{self.source_bucket}/{self.source_key}"
        dest_key_s3_url = f"s3://{self.dest_bucket}/{self.dest_key}"

        op = S3CopyObjectOperator(
            task_id="test",
            source_bucket_key=source_key_s3_url,
            dest_bucket_key=dest_key_s3_url,
        )

        lineage = op.get_openlineage_facets_on_start()
        assert len(lineage.inputs) == 1
        assert len(lineage.outputs) == 1
        assert lineage.inputs[0] == expected_input
        assert lineage.outputs[0] == expected_output

    def test_template_fields(self):
        operator = S3CopyObjectOperator(
            task_id="test_task_s3_copy_object",
            source_bucket_key=self.source_key,
            source_bucket_name=self.source_bucket,
            dest_bucket_key=self.dest_key,
            dest_bucket_name=self.dest_bucket,
        )
        validate_template_fields(operator)


@mock_aws
class TestS3DeleteObjectsOperator:
    def test_s3_delete_single_object(self):
        bucket = "testbucket"
        key = "path/data.txt"

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket, Key=key, Fileobj=BytesIO(b"input"))

        # The object should be detected before the DELETE action is taken
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket, Prefix=key)
        assert len(objects_in_dest_bucket["Contents"]) == 1
        assert objects_in_dest_bucket["Contents"][0]["Key"] == key

        op = S3DeleteObjectsOperator(task_id="test_task_s3_delete_single_object", bucket=bucket, keys=key)
        op.execute(None)

        # There should be no object found in the bucket created earlier
        assert "Contents" not in conn.list_objects(Bucket=bucket, Prefix=key)

    def test_s3_delete_multiple_objects(self):
        bucket = "testbucket"
        key_pattern = "path/data"
        n_keys = 3
        keys = [key_pattern + str(i) for i in range(n_keys)]

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)
        for k in keys:
            conn.upload_fileobj(Bucket=bucket, Key=k, Fileobj=BytesIO(b"input"))

        # The objects should be detected before the DELETE action is taken
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket, Prefix=key_pattern)
        assert len(objects_in_dest_bucket["Contents"]) == n_keys
        assert sorted(x["Key"] for x in objects_in_dest_bucket["Contents"]) == sorted(keys)

        op = S3DeleteObjectsOperator(task_id="test_task_s3_delete_multiple_objects", bucket=bucket, keys=keys)
        op.execute(None)

        # There should be no object found in the bucket created earlier
        assert "Contents" not in conn.list_objects(Bucket=bucket, Prefix=key_pattern)

    @pytest.mark.db_test
    def test_dates_from_template(self, session, testing_dag_bundle):
        """Specifically test for dates passed from templating that could be strings"""
        bucket = "testbucket"
        key_pattern = "path/data"
        n_keys = 3
        keys = [key_pattern + str(i) for i in range(n_keys)]

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)
        for k in keys:
            conn.upload_fileobj(Bucket=bucket, Key=k, Fileobj=BytesIO(b"input"))

        logical_date = utcnow()
        dag = DAG("test_dag", start_date=datetime(2020, 1, 1), schedule=timedelta(days=1))
        # use macros.ds_add since it returns a string, not a date
        op = S3DeleteObjectsOperator(
            task_id="XXXXXXXXXXXXXXXXXXXXXXX",
            bucket=bucket,
            from_datetime="{{ macros.ds_add(ds, -1) }}",
            to_datetime="{{ macros.ds_add(ds, 1) }}",
            dag=dag,
        )
        if hasattr(DagRun, "execution_date"):  # Airflow 2.x.
            dag_run = DagRun(
                dag_id=dag.dag_id,
                execution_date=logical_date,
                run_id="test",
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING,
            )
        else:
            dag_run = DagRun(
                dag_id=dag.dag_id,
                logical_date=logical_date,
                run_id="test",
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING,
            )
        if AIRFLOW_V_3_0_PLUS:
            from airflow.models.dag_version import DagVersion

            sync_dag_to_db(dag)
            dag_version = DagVersion.get_latest_version(dag.dag_id)
            ti = TaskInstance(task=op, dag_version_id=dag_version.id)
        else:
            ti = TaskInstance(task=op)
        ti.dag_run = dag_run
        session.add(ti)
        session.commit()
        context = ti.get_template_context(session)

        ti.render_templates(context)
        op.execute(None)
        assert "Contents" not in conn.list_objects(Bucket=bucket)

    def test_s3_delete_from_to_datetime(self):
        bucket = "testbucket"
        key_pattern = "path/data"
        n_keys = 3
        keys = [key_pattern + str(i) for i in range(n_keys)]

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)
        for k in keys:
            conn.upload_fileobj(Bucket=bucket, Key=k, Fileobj=BytesIO(b"input"))

        # The objects should be detected before the DELETE action is taken
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket)
        assert len(objects_in_dest_bucket["Contents"]) == n_keys
        assert sorted(x["Key"] for x in objects_in_dest_bucket["Contents"]) == sorted(keys)

        now = utcnow()
        from_datetime = now.replace(year=now.year - 1)
        to_datetime = now.replace(year=now.year + 1)

        op = S3DeleteObjectsOperator(
            task_id="test_task_s3_delete_prefix",
            bucket=bucket,
            from_datetime=from_datetime,
            to_datetime=to_datetime,
        )
        op.execute(None)

        # There should be no object found in the bucket created earlier
        assert "Contents" not in conn.list_objects(Bucket=bucket)

    def test_s3_delete_prefix(self):
        bucket = "testbucket"
        key_pattern = "path/data"
        n_keys = 3
        keys = [key_pattern + str(i) for i in range(n_keys)]

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)
        for k in keys:
            conn.upload_fileobj(Bucket=bucket, Key=k, Fileobj=BytesIO(b"input"))

        # The objects should be detected before the DELETE action is taken
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket, Prefix=key_pattern)
        assert len(objects_in_dest_bucket["Contents"]) == n_keys
        assert sorted(x["Key"] for x in objects_in_dest_bucket["Contents"]) == sorted(keys)

        op = S3DeleteObjectsOperator(task_id="test_task_s3_delete_prefix", bucket=bucket, prefix=key_pattern)
        op.execute(None)

        # There should be no object found in the bucket created earlier
        assert "Contents" not in conn.list_objects(Bucket=bucket, Prefix=key_pattern)

    def test_s3_delete_empty_list(self):
        bucket = "testbucket"
        key_of_test = "path/data.txt"
        keys = []

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket, Key=key_of_test, Fileobj=BytesIO(b"input"))

        # The object should be detected before the DELETE action is tested
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket, Prefix=key_of_test)
        assert len(objects_in_dest_bucket["Contents"]) == 1
        assert objects_in_dest_bucket["Contents"][0]["Key"] == key_of_test

        op = S3DeleteObjectsOperator(task_id="test_s3_delete_empty_list", bucket=bucket, keys=keys)
        op.execute(None)

        # The object found in the bucket created earlier should still be there
        assert len(objects_in_dest_bucket["Contents"]) == 1
        # the object found should be consistent with dest_key specified earlier
        assert objects_in_dest_bucket["Contents"][0]["Key"] == key_of_test

    def test_s3_delete_empty_string(self):
        bucket = "testbucket"
        key_of_test = "path/data.txt"
        keys = ""

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket, Key=key_of_test, Fileobj=BytesIO(b"input"))

        # The object should be detected before the DELETE action is tested
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket, Prefix=key_of_test)
        assert len(objects_in_dest_bucket["Contents"]) == 1
        assert objects_in_dest_bucket["Contents"][0]["Key"] == key_of_test

        op = S3DeleteObjectsOperator(task_id="test_s3_delete_empty_string", bucket=bucket, keys=keys)
        op.execute(None)

        # The object found in the bucket created earlier should still be there
        assert len(objects_in_dest_bucket["Contents"]) == 1
        # the object found should be consistent with dest_key specified earlier
        assert objects_in_dest_bucket["Contents"][0]["Key"] == key_of_test

    @pytest.mark.parametrize(
        ("keys", "prefix", "from_datetime", "to_datetime"),
        [
            pytest.param("path/data.txt", "path/data", None, None, id="single-key-and-prefix"),
            pytest.param(["path/data.txt"], "path/data", None, None, id="multiple-keys-and-prefix"),
            pytest.param(
                ["path/data.txt"],
                "path/data",
                datetime(1992, 3, 8, 18, 52, 51),
                None,
                id="keys-prefix-and-from_datetime",
            ),
            pytest.param(
                ["path/data.txt"],
                "path/data",
                datetime(1992, 3, 8, 18, 52, 51),
                datetime(1993, 3, 8, 18, 52, 51),
                id="keys-prefix-and-from-to_datetime",
            ),
            pytest.param(None, None, None, None, id="all-none"),
        ],
    )
    def test_validate_keys_and_filters_in_constructor(self, keys, prefix, from_datetime, to_datetime):
        with pytest.raises(
            AirflowException,
            match=r"Either keys or at least one of prefix, from_datetime, to_datetime should be set.",
        ):
            S3DeleteObjectsOperator(
                task_id="test_validate_keys_and_prefix_in_constructor",
                bucket="foo-bar-bucket",
                keys=keys,
                prefix=prefix,
                from_datetime=from_datetime,
                to_datetime=to_datetime,
            )

    @pytest.mark.parametrize(
        ("keys", "prefix", "from_datetime", "to_datetime"),
        [
            pytest.param("path/data.txt", "path/data", None, None, id="single-key-and-prefix"),
            pytest.param(["path/data.txt"], "path/data", None, None, id="multiple-keys-and-prefix"),
            pytest.param(
                ["path/data.txt"],
                "path/data",
                datetime(1992, 3, 8, 18, 52, 51),
                None,
                id="keys-prefix-and-from_datetime",
            ),
            pytest.param(
                ["path/data.txt"],
                "path/data",
                datetime(1992, 3, 8, 18, 52, 51),
                datetime(1993, 3, 8, 18, 52, 51),
                id="keys-prefix-and-from-to_datetime",
            ),
            pytest.param(None, None, None, None, id="all-none"),
        ],
    )
    def test_validate_keys_and_prefix_in_execute(self, keys, prefix, from_datetime, to_datetime):
        bucket = "testbucket"
        key_of_test = "path/data.txt"

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket, Key=key_of_test, Fileobj=BytesIO(b"input"))

        # Set valid values for constructor, and change them later for emulate rendering template
        op = S3DeleteObjectsOperator(
            task_id="test_validate_keys_and_prefix_in_execute",
            bucket=bucket,
            keys="keys-exists",
            prefix=None,
        )
        op.keys = keys
        op.prefix = prefix
        op.from_datetime = from_datetime
        op.to_datetime = to_datetime

        # The object should be detected before the DELETE action is tested
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket, Prefix=key_of_test)
        assert len(objects_in_dest_bucket["Contents"]) == 1
        assert objects_in_dest_bucket["Contents"][0]["Key"] == key_of_test

        with pytest.raises(
            AirflowException,
            match=r"Either keys or at least one of prefix, from_datetime, to_datetime should be set.",
        ):
            op.execute(None)

        # The object found in the bucket created earlier should still be there
        assert len(objects_in_dest_bucket["Contents"]) == 1
        # the object found should be consistent with dest_key specified earlier
        assert objects_in_dest_bucket["Contents"][0]["Key"] == key_of_test

    @pytest.mark.parametrize("keys", ("path/data.txt", ["path/data.txt"]))
    def test_get_openlineage_facets_on_complete_single_object(self, keys):
        bucket = "testbucket"
        expected_input = Dataset(
            namespace=f"s3://{bucket}",
            name="path/data.txt",
            facets={
                "lifecycleStateChange": LifecycleStateChangeDatasetFacet(
                    lifecycleStateChange=LifecycleStateChange.DROP.value,
                    previousIdentifier=PreviousIdentifier(
                        namespace=f"s3://{bucket}",
                        name="path/data.txt",
                    ),
                )
            },
        )

        op = S3DeleteObjectsOperator(task_id="test_task_s3_delete_single_object", bucket=bucket, keys=keys)
        op.hook = mock.MagicMock()
        op.execute(None)

        lineage = op.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0] == expected_input

    def test_get_openlineage_facets_on_complete_multiple_objects(self):
        bucket = "testbucket"
        keys = ["path/data1.txt", "path/data2.txt"]
        expected_inputs = [
            Dataset(
                namespace=f"s3://{bucket}",
                name="path/data1.txt",
                facets={
                    "lifecycleStateChange": LifecycleStateChangeDatasetFacet(
                        lifecycleStateChange=LifecycleStateChange.DROP.value,
                        previousIdentifier=PreviousIdentifier(
                            namespace=f"s3://{bucket}",
                            name="path/data1.txt",
                        ),
                    )
                },
            ),
            Dataset(
                namespace=f"s3://{bucket}",
                name="path/data2.txt",
                facets={
                    "lifecycleStateChange": LifecycleStateChangeDatasetFacet(
                        lifecycleStateChange=LifecycleStateChange.DROP.value,
                        previousIdentifier=PreviousIdentifier(
                            namespace=f"s3://{bucket}",
                            name="path/data2.txt",
                        ),
                    )
                },
            ),
        ]

        op = S3DeleteObjectsOperator(task_id="test_task_s3_delete_single_object", bucket=bucket, keys=keys)
        op.hook = mock.MagicMock()
        op.execute(None)

        lineage = op.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 2
        assert lineage.inputs == expected_inputs

    @pytest.mark.parametrize("keys", ("", []))
    @mock.patch("airflow.providers.amazon.aws.operators.s3.S3Hook")
    def test_get_openlineage_facets_on_complete_no_objects(self, mock_hook, keys):
        op = S3DeleteObjectsOperator(
            task_id="test_task_s3_delete_single_object", bucket="testbucket", keys=keys
        )
        op.execute(None)

        lineage = op.get_openlineage_facets_on_complete(None)
        assert lineage == OperatorLineage()

    def test_template_fields(self):
        operator = S3DeleteObjectsOperator(
            task_id="test_task_s3_delete_single_object", bucket="test-bucket", keys="test/file.csv"
        )
        validate_template_fields(operator)


class TestS3CreateObjectOperator:
    @mock.patch.object(S3Hook, "load_string")
    def test_execute_if_data_is_string(self, mock_load_string):
        data = "data"
        operator = S3CreateObjectOperator(
            task_id="test-s3-operator",
            s3_bucket=BUCKET_NAME,
            s3_key=S3_KEY,
            data=data,
        )
        operator.execute(None)

        mock_load_string.assert_called_once_with(data, S3_KEY, BUCKET_NAME, False, False, None, None, None)

    @mock.patch.object(S3Hook, "load_bytes")
    def test_execute_if_data_is_bytes(self, mock_load_bytes):
        data = b"data"
        operator = S3CreateObjectOperator(
            task_id="test-s3-create-object-operator",
            s3_bucket=BUCKET_NAME,
            s3_key=S3_KEY,
            data=data,
        )
        operator.execute(None)

        mock_load_bytes.assert_called_once_with(data, S3_KEY, BUCKET_NAME, False, False, None)

    @mock.patch.object(S3Hook, "load_string")
    def test_execute_if_s3_bucket_not_provided(self, mock_load_string):
        data = "data"
        operator = S3CreateObjectOperator(
            task_id="test-s3-create-object-operator",
            s3_key=f"s3://{BUCKET_NAME}/{S3_KEY}",
            data=data,
        )
        operator.execute(None)

        mock_load_string.assert_called_once_with(data, S3_KEY, BUCKET_NAME, False, False, None, None, None)

    @pytest.mark.parametrize(("bucket", "key"), (("bucket", "file.txt"), (None, "s3://bucket/file.txt")))
    def test_get_openlineage_facets_on_start(self, bucket, key):
        expected_output = Dataset(
            namespace="s3://bucket",
            name="file.txt",
        )

        op = S3CreateObjectOperator(task_id="test", s3_bucket=bucket, s3_key=key, data="test")

        lineage = op.get_openlineage_facets_on_start()
        assert len(lineage.inputs) == 0
        assert len(lineage.outputs) == 1
        assert lineage.outputs[0] == expected_output

    def test_template_fields(self):
        operator = S3CreateObjectOperator(task_id="test", s3_bucket="bucket", s3_key="key", data="test")
        validate_template_fields(operator)
