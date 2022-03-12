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
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep


class MappedTaskIsExpanded(BaseTIDep):
    """Checks that a mapped task has been expanded before it's TaskInstance can run."""

    NAME = "Task has been mapped"
    IGNORABLE = False
    IS_TASK_DEP = False

    def _get_dep_statuses(self, ti, session, dep_context):
        if dep_context.ignore_unmapped_tasks:
            return
        if ti.map_index == -1:
            yield self._failing_status(reason="The task has yet to be mapped!")
            return
        yield self._passing_status(reason="The task has been mapped")
