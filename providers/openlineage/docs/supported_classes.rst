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


.. _supported_classes:openlineage:

Supported classes
===================

Below is a list of Operators and Hooks that support OpenLineage extraction, along with specific DB types that are compatible with the supported SQL operators.

.. important::

    While we strive to keep the list of supported classes current,
    please be aware that our updating process is automated and may not always capture everything accurately.
    Detecting hook level lineage is challenging so make sure to double check the information provided below.

What does "supported operator" mean?
-------------------------------------

**All Airflow operators will automatically emit OpenLineage events**, (unless explicitly disabled or skipped during
scheduling, like EmptyOperator) regardless of whether they appear on the "supported" list.
Every OpenLineage event will contain basic information such as:

- Task and DAG run metadata (execution time, state, tags, parameters, owners, description, etc.)
- Job relationship (DAG job that the task belongs to, upstream/downstream relationship between tasks in a DAG etc.)
- Error message (in case of task failure)
- Airflow and OpenLineage provider versions

**"Supported" operators provide additional metadata** that enhances the lineage information:

- **Input and output datasets** (sometimes with Column Level Lineage)
- **Operator-specific details** that may include SQL query text and query IDs, source code, job IDs from external systems (e.g., Snowflake or BigQuery job ID), data quality metrics and other information.

For example, a supported SQL operator will include the executed SQL query, query ID, and input/output table information
in its OpenLineage events. An unsupported operator will still appear in the lineage graph, but without these details.

.. tip::

  You can easily implement OpenLineage support for any operator. See :ref:`guides/developer:openlineage`.

.. airflow-providers-openlineage-supported-classes::
