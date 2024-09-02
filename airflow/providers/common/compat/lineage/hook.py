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


def get_hook_lineage_collector():
    # HookLineageCollector added in 2.10
    try:
        from airflow.lineage.hook import get_hook_lineage_collector

        return get_hook_lineage_collector()
    except ImportError:

        class NoOpCollector:
            """
            NoOpCollector is a hook lineage collector that does nothing.

            It is used when you want to disable lineage collection.
            """

            def add_input_dataset(self, *_, **__):
                pass

            def add_output_dataset(self, *_, **__):
                pass

        return NoOpCollector()
