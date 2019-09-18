#!/usr/bin/env python
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
from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor
from airflow.utils.file import TemporaryDirectory


class DataprocTestHelper(LoggingCommandExecutor):
    def create_test_bucket(self, bucket: str):
        self.execute_cmd(["gsutil", "mb", "gs://{bucket}/".format(bucket=bucket)], True)

    def upload_test_file(self, uri: str, file_name: str):
        with TemporaryDirectory(prefix="airflow-gcp") as tmp_dir:
            # 1. Create required files
            quickstart_path = os.path.join(tmp_dir, file_name)
            with open(quickstart_path, "w") as file:
                file.writelines(
                    [
                        "#!/usr/bin/python\n",
                        "import pyspark\n",
                        "sc = pyspark.SparkContext()\n",
                        "rdd = sc.parallelize(['Hello,', 'world!'])\n",
                        "words = sorted(rdd.collect())\n",
                        "print(words)\n",
                    ]
                )
                file.flush()

            os.chmod(quickstart_path, 555)

            self.execute_cmd(
                [
                    "gsutil",
                    "cp",
                    "{file}".format(file=quickstart_path),
                    "{uri}".format(uri=uri),
                ]
            )

    def delete_gcs_bucket_elements(self, bucket: str):
        self.execute_cmd(
            ["gsutil", "rm", "-r", "gs://{bucket}/".format(bucket=bucket)], True
        )
