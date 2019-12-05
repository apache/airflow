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

from collections import namedtuple

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

# Namedtuple for testing purposes
MockNamedTuple = namedtuple("MockNamedTuple", ["var1", "var2"])


class MockOperator(BaseOperator):
    """Operator for testing purposes."""

    template_fields = ("arg1", "arg2")

    @apply_defaults
    def __init__(self, arg1: str = "", arg2: str = "", **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1
        self.arg2 = arg2

    def execute(self, context):
        pass
