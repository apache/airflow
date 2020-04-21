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
from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor


class GcpBigqueryDtsTestHelper(LoggingCommandExecutor):
    def create_dataset(self, project_id: str, dataset: str, table: str):
        dataset_name = "{}:{}".format(project_id, dataset)
        self.execute_cmd(["bq", "--location", "us", "mk", "--dataset", dataset_name])
        table_name = "{}.{}".format(dataset_name, table)
        self.execute_cmd(["bq", "mk", "--table", table_name, ""])

    def upload_data(self, dataset: str, table: str, gcs_file: str):
        table_name = "{}.{}".format(dataset, table)
        self.execute_cmd(
            [
                "bq",
                "--location",
                "us",
                "load",
                "--autodetect",
                "--source_format",
                "CSV",
                table_name,
                gcs_file,
            ]
        )

    def delete_dataset(self, project_id: str, dataset: str):
        dataset_name = "{}:{}".format(project_id, dataset)
        self.execute_cmd(["bq", "rm", "-r", "-f", "-d", dataset_name])
