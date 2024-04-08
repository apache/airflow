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

.. _howto/macros:openlineage:

OpenLineage Macros
==================

Macros included in OpenLineage plugin get integrated to Airflow's main collections and become available for use.
They can be invoked as a Jinja template.

Lineage parent id
-----------------
Forwarding task information from a task to a child run, to preserve the job hierarchy, can be done in two ways.

Separate macros allow injecting pieces of run information of a given Airflow task into the arguments
sent to a remote processing job:

- ``lineage_job_namespace()``
- ``lineage_job_name(task_instance)``
- ``lineage_run_id(task_instance)``

For example, ``SparkSubmitOperator`` can be set up like this:

.. code-block:: python

        t1 = SparkSubmitOperator(
            task_id="my_task",
            application="/script.py"
            conf={
                "spark.openlineage.parentJobNamespace": "{{ macros.OpenLineagePlugin.lineage_job_namespace() }}",
                "spark.openlineage.parentJobName": "{{ macros.OpenLineagePlugin.lineage_job_name(task_instance) }}",
                "spark.openlineage.parentRunId": "{{ macros.OpenLineagePlugin.lineage_run_id(task_instance) }}",
            },
        )


Same information, but compacted to one string, can be passed using single macro:

- ``linage_parent_run_id(task_instance)``

For example, ``SparkSubmitOperator`` can be set up like this:

.. code-block:: python

        t1 = SparkSubmitOperator(
            task_id="my_task",
            application="/script.py"
            conf={
                "spark.openlineage.parentId": "{{ macros.OpenLineagePlugin.lineage_parent_id(task_instance) }}",
            },
        )
