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

Invoke as a jinja template, e.g.

Lineage run id
--------------
.. code-block:: python

        PythonOperator(
            task_id="render_template",
            python_callable=my_task_function,
            op_args=["{{ lineage_run_id(task, task_instance) }}"],  # lineage_run_id macro invoked
            provide_context=False,
            dag=dag,
        )

Lineage parent id
-----------------
.. code-block:: python

        PythonOperator(
            task_id="render_template",
            python_callable=my_task_function,
            op_args=["{{ lineage_parent_id(run_id, task_instance) }}"],  # macro invoked
            provide_context=False,
            dag=dag,
        )
