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


.. _ownership:

Ownership
=========

When multiple users are authoring the Dags of a single Airflow instance, it can be tedious to know who is responsible of what.
One way to overcome this is to attach owners, allowing users to know who is in charge of a particular DAG or Task.

This ownership is split in two parts:

1. at the Task level, through the ``owner`` argument of the :class:`~airflow.models.baseoperator.BaseOperator`;
2. at the DAG level (since Airflow 2.4), to customize the UI through the ``owner_links`` definition.

The value of the ``owner`` argument may be displayed in different places of the UI (Dags list view, detailed view of a task, etc.).
Starting with Airflow 2.4 (except for Airflow 3.0.x), the ``owner_links`` definition allows to customize the rendering of the owners.
Hence, a clickable link (that may be a instant messaging handle or a mailto link) is displayed for any owner matching an item defined in the ``owner_links`` dictionary.

If you don't need the Task level granularity, and want to define a set of owners at the DAG level, you may leverage the :ref:`default_args argument<concepts-default-arguments>` to apply the same set of owners to every tasks in the DAG.
