 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.



Lineage
========

.. note:: Lineage support is very experimental and subject to change.

Airflow provides a powerful feature for tracking data lineage not only between tasks but also from hooks used within those tasks.
This functionality helps you understand how data flows throughout your Airflow pipelines.

A global instance of ``HookLineageCollector`` serves as the central hub for collecting lineage information.
Hooks can send details about assets they interact with to this collector.
The collector then uses this data to construct AIP-60 compliant Assets, a standard format for describing assets.
Hooks can also send arbitrary non-asset related data to this collector as shown in the example below.

.. code-block:: python

    from airflow.lineage.hook import get_hook_lineage_collector


    class CustomHook(BaseHook):
        def run(self):
            # run actual code
            collector = get_hook_lineage_collector()
            collector.add_input_asset(self, asset_kwargs={"scheme": "file", "path": "/tmp/in"})
            collector.add_output_asset(self, asset_kwargs={"scheme": "file", "path": "/tmp/out"})
            collector.add_extra(self, key="external_system_job_id", value="some_id_123")

Lineage data collected by the ``HookLineageCollector`` can be accessed using an instance of ``HookLineageReader``,
which is registered in an Airflow plugin.

.. code-block:: python

    from airflow.lineage.hook_lineage import HookLineageReader
    from airflow.plugins_manager import AirflowPlugin


    class CustomHookLineageReader(HookLineageReader):
        def get_inputs(self):
            return self.lineage_collector.collected_assets.inputs


    class HookLineageCollectionPlugin(AirflowPlugin):
        name = "HookLineageCollectionPlugin"
        hook_lineage_readers = [CustomHookLineageReader]

If no ``HookLineageReader`` is registered within Airflow, a default ``NoOpCollector`` is used instead.
This collector does not create AIP-60 compliant assets or collect lineage information.
