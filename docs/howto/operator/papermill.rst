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



Papermill
---------

Apache Airflow supports integration with Papermill_. Papermill is a tool for
parameterizing and executing Jupyter Notebooks. Perhaps you have a financial
report that you wish to run with different values on the first or last day of
a month or at the beginning or end of the year. Using *parameters* in your
notebook and using the *PapermillOperator* makes this a breeze.

.. _Papermill: https://papermill.readthedocs.io/en/latest/


Usage
=====

Creating a notebook
'''''''''''''''''''

To parameterize your notebook designate a cell with the tag parameters. Papermill
looks for the parameters cell and treats this cell as defaults for the parameters
passed in at execution time. Papermill will add a new cell tagged with injected-parameters
with input parameters in order to overwrite the values in parameters. If no cell is
tagged with parameters the injected cell will be inserted at the top of the notebook.

Make sure that you save your notebook somewhere so that Airflow can access it. Papermill
supports S3, GCS, Azure and Local. HDFS is *not* supported.

Example DAG
'''''''''''

Use the :class:`~airflow.providers.papermill.operators.papermill.PapermillOperator`
to execute a jupyter notebook:

.. exampleinclude:: /../airflow/providers/papermill/example_dags/example_papermill.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_papermill]
    :end-before: [END howto_operator_papermill]
