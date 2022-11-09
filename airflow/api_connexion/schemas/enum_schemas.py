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

from marshmallow import fields, validate

from airflow.utils.state import State


class DagStateField(fields.String):
    """Schema for DagState Enum"""

    def __init__(self, **metadata):
        super().__init__(**metadata)
        self.validators = [validate.OneOf(State.dag_states)] + list(self.validators)


class TaskInstanceStateField(fields.String):
    """Schema for TaskInstanceState Enum"""

    def __init__(self, **metadata):
        super().__init__(**metadata)
        self.validators = [validate.OneOf(State.task_states)] + list(self.validators)
