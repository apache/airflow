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

from tests_common.test_utils.system_tests_class import SystemTest


class Terraform(SystemTest):
    """Base class for Terraform tests."""

    TERRAFORM_DIR: str

    def setup_method(self) -> None:
        self.execute_cmd(["terraform", "init", "-input=false", self.TERRAFORM_DIR])
        self.execute_cmd(["terraform", "plan", "-input=false", self.TERRAFORM_DIR])
        self.execute_cmd(
            ["terraform", "apply", "-input=false", "-auto-approve", self.TERRAFORM_DIR]
        )

    def get_tf_output(self, name):
        return "".join(
            self.check_output(["terraform", "output", name]).decode("utf-8").splitlines()
        )

    def teardown_method(self) -> None:
        self.execute_cmd(
            ["terraform", "plan", "-destroy", "-input=false", self.TERRAFORM_DIR]
        )
        self.execute_cmd(
            ["terraform", "destroy", "-input=false", "-auto-approve", self.TERRAFORM_DIR]
        )
