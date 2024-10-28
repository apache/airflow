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

from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest

from airflow.providers.amazon.aws.operators.glacier import (
    GlacierCreateJobOperator,
    GlacierUploadArchiveOperator,
)

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator

AWS_CONN_ID = "aws_default"
BUCKET_NAME = "airflow_bucket"
FILENAME = "path/to/file/"
GCP_CONN_ID = "google_cloud_default"
JOB_ID = "1a2b3c4d"
OBJECT_NAME = "file.csv"
TASK_ID = "glacier_job"
VAULT_NAME = "airflow"


class BaseGlacierOperatorsTests:
    op_class: type[AwsBaseOperator]
    default_op_kwargs: dict[str, Any]

    def test_base_aws_op_attributes(self):
        op = self.op_class(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = self.op_class(
            **self.default_op_kwargs,
            aws_conn_id="aws-test-custom-conn",
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.aws_conn_id == "aws-test-custom-conn"
        assert op.hook._region_name == "eu-west-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42


class TestGlacierCreateJobOperator(BaseGlacierOperatorsTests):
    op_class = GlacierCreateJobOperator

    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        self.default_op_kwargs = {"vault_name": VAULT_NAME, "task_id": TASK_ID}

    @mock.patch.object(GlacierCreateJobOperator, "hook", new_callable=mock.PropertyMock)
    def test_execute(self, hook_mock):
        op = self.op_class(aws_conn_id=None, **self.default_op_kwargs)
        op.execute(mock.MagicMock())
        hook_mock.return_value.retrieve_inventory.assert_called_once_with(
            vault_name=VAULT_NAME
        )

    def test_template_fields(self):
        operator = self.op_class(**self.default_op_kwargs)
        validate_template_fields(operator)


class TestGlacierUploadArchiveOperator(BaseGlacierOperatorsTests):
    op_class = GlacierUploadArchiveOperator

    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        self.default_op_kwargs = {
            "vault_name": VAULT_NAME,
            "task_id": TASK_ID,
            "body": b"Test Data",
        }

    def test_execute(self):
        with mock.patch.object(
            self.op_class.aws_hook_class, "conn", new_callable=mock.PropertyMock
        ) as m:
            op = self.op_class(aws_conn_id=None, **self.default_op_kwargs)
            op.execute(mock.MagicMock())
            m.return_value.upload_archive.assert_called_once_with(
                accountId=None,
                vaultName=VAULT_NAME,
                archiveDescription=None,
                body=b"Test Data",
                checksum=None,
            )

    def test_template_fields(self):
        operator = self.op_class(**self.default_op_kwargs)
        validate_template_fields(operator)
