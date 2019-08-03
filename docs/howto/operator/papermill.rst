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

Note that Jupyter notebook has out of the box support for tags but you need to install
the celltags extension for Jupyter Lab: ``jupyter labextension install @jupyterlab/celltags``

Make sure that you save your notebook somewhere so that Airflow can access it. Papermill
supports S3, GCS, Azure and Local. HDFS is *not* supported.

Example DAG
'''''''''''

.. code:: python

    import airflow

    from airflow.models import DAG
    from airflow.operators.papermill_operator import PapermillOperator

    from datetime import timedelta

    args = {
        'owner': 'Airflow',
        'start_date': airflow.utils.dates.days_ago(2)
    }

    dag = DAG(
        dag_id='example_papermill_operator', default_args=args,
        schedule_interval='0 0 * * *',
        dagrun_timeout=timedelta(minutes=60))

    run_this = PapermillOperator(
        task_id="run_example_notebook",
        dag=dag,
        input_nb="/tmp/hello_world.ipynb",
        output_nb="/tmp/out-{{ execution_date }}.ipynb",
        parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"}
    )

This DAG will use Papermill to run the notebook "hello_world", based on the execution date
it will create an output notebook "out-<date>". All fields, including the keys in the parameters, are
templated.
