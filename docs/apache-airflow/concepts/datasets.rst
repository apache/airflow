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

Datasets
========

.. versionadded:: 2.4

With datasets, instead of running a DAG on a schedule, a DAG can be configured to run when a dataset has been updated.

To use this feature, define a dataset:

.. exampleinclude:: /../../airflow/example_dags/example_datasets.py
    :language: python
    :start-after: [START dataset_def]
    :end-before: [END dataset_def]

Then reference the dataset as a task outlet:

.. exampleinclude:: /../../airflow/example_dags/example_datasets.py
    :language: python
    :dedent: 4
    :start-after: [START task_outlet]
    :end-before: [END task_outlet]

Finally, define a DAG and reference this dataset in the DAG's ``schedule`` argument:

.. exampleinclude:: /../../airflow/example_dags/example_datasets.py
    :language: python
    :start-after: [START dag_dep]
    :end-before: [END dag_dep]

You can reference multiple datasets in the DAG's ``schedule`` argument.  Once there has been an update to all of the upstream datasets, the DAG will be triggered.  This means that the DAG will run as frequently as its least-frequently-updated dataset.
