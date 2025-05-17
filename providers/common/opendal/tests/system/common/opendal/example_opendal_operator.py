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

from airflow.sdk import DAG

with DAG(
    dag_id="example_opendal_operator",
    catchup=False,
) as dag:
    # [START howto_read_operator_opendal]
    from airflow.providers.common.opendal.operators.opendal import OpenDALTaskOperator

    read_task = OpenDALTaskOperator(
        task_id="read_opendal",
        action="read",
        opendal_config={  # type: ignore
            "source_config": {
                "operator_args": {
                    "scheme": "fs",
                },
                "path": "/tmp/to/file.txt",
            }
        },
    )
    # [END howto_read_operator_opendal]

    # [START howto_write_operator_opendal]
    write_task = OpenDALTaskOperator(
        task_id="write_opendal",
        action="write",
        opendal_config={  # type: ignore
            "source_config": {
                "operator_args": {
                    "scheme": "fs",
                },
                "path": "/tmp/to/file.txt",
            },
            "data": b"Hello, World!",
        },
    )
    # [END howto_write_operator_opendal]

    # [START howto_copy_operator_opendal]
    copy_task = OpenDALTaskOperator(
        task_id="copy_opendal",
        action="copy",
        opendal_config={  # type: ignore
            "source_config": {
                "operator_args": {
                    "scheme": "fs",
                },
                "path": "/tmp/to/file.txt",
            },
            "destination_config": {
                "operator_args": {
                    "scheme": "fs",
                },
                "path": "/tmp/to/file_copy.txt",
            },
        },
    )
    # [END howto_copy_operator_opendal]

    # [START howto_copy_from_aws_to_gcs]
    copy_task_aws_to_gcs = OpenDALTaskOperator(
        task_id="copy_opendal_aws_to_gcs",
        action="copy",
        opendal_config={  # type: ignore
            "source_config": {
                "conn_id": "aws_default",
                "operator_args": {
                    "scheme": "s3",
                    "bucket": "aws-bucket",
                },
                "path": "/tmp/to/file.txt",
            },
            "destination_config": {
                "conn_id": "gcs_default",
                "operator_args": {
                    "scheme": "gs",
                    "bucket": "gcs-bucket",
                },
                "path": "/tmp/to/file_copy.txt",
            },
        },
    )
    # [END howto_copy_from_aws_to_gcs]


from tests_common.test_utils.system_tests import get_test_run

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
