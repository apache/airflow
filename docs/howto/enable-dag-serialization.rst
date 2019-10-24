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




Enable DAG Serialization
========================

Add the following settings in ``airflow.cfg``:

.. code-block:: ini

    [core]
    store_serialized_dags = True
    min_serialized_dag_update_interval = 30

*   ``store_serialized_dags``: This flag decides whether to serialises DAGs and persist them in DB.
    If set to True, Webserver reads from DB instead of parsing DAG files
*   ``min_serialized_dag_update_interval``: This flag sets the minimum interval (in seconds) after which
    the serialized DAG in DB should be updated. This helps in reducing database write rate.

If you are updating Airflow from <1.10.6, please do not forget to run ``airflow db upgrade``.


How it works
------------

In order to make Airflow Webserver stateless (almost!), Airflow >=1.10.6 supports
DAG Serialization and DB Persistence.

.. image:: ../img/dag_serialization.png

As shown in the image above in Vanilla Airflow, the Webserver and the Scheduler both
needs access to the DAG files. Both the scheduler and webserver parses the DAG files.

With **DAG Serialization** we aim to decouple the webserver from DAG parsing
which would make the Webserver very light-weight.

As shown in the image above, when using the dag_serilization feature,
the Scheduler parses the DAG files, serializes them in JSON format and saves them in the Metadata DB.

The Webserver now instead of having to parse the DAG file again, reads the
serialized DAGs in JSON, de-serializes them and create the DagBag and uses it
to show in the UI.

One of the key features that is implemented as the part of DAG Serialization is that
instead of loading an entire DagBag when the WebServer starts we only load each DAG on demand from the
Serialized Dag table. This helps reduce Webserver startup time and memory. The reduction is notable
when you have large number of DAGs.

Below is the screenshot of the ``serialized_dag`` table in Metadata DB:

.. image:: ../img/serialized_dag_table.png

Limitations
-----------
The Webserver will still need access to DAG files in the following cases,
which is why we said "almost" stateless.

*   **Rendered Template** tab will still have to parse Python file as it needs all the details like
    the execution date and even the data passed by the upstream task using Xcom.
*   **Code View** will read the DAG File & show it using Pygments.
    However, it does not need to Parse the Python file so it is still a small operation.
*   :doc:`Extra Operator Links <define_extra_link>` would not work out of
    the box. They need to be defined in Airflow Plugin.

    **Existing Airflow Operators**:
    To make extra operator links work with existing operators like BigQuery, copy all
    the classes that are defined in ``operator_extra_links`` property.

    For :class:`~airflow.gcp.operators.bigquery.BigQueryOperator` you can create the following plugin for extra links:

    .. code-block:: python

        from airflow.plugins_manager import AirflowPlugin
        from airflow.models.baseoperator import BaseOperatorLink
        from airflow.gcp.operators.bigquery import BigQueryOperator

        class BigQueryConsoleLink(BaseOperatorLink):
            """
            Helper class for constructing BigQuery link.
            """
            name = 'BigQuery Console'
            operators = [BigQueryOperator]

            def get_link(self, operator, dttm):
                ti = TaskInstance(task=operator, execution_date=dttm)
                job_id = ti.xcom_pull(task_ids=operator.task_id, key='job_id')
                return BIGQUERY_JOB_DETAILS_LINK_FMT.format(job_id=job_id) if job_id else ''

        # Defining the plugin class
        class AirflowExtraLinkPlugin(AirflowPlugin):
            name = "extra_link_plugin"
            operator_extra_links = [BigQueryConsoleLink(), ]
