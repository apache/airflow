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




Define an operator extra link
=============================

.. image:: ../img/operator_extra_link.png

The following code shows how to add extra links to an operator via Plugins:

.. code-block:: python

    from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
    from airflow.plugins_manager import AirflowPlugin


    class GoogleLink(BaseOperatorLink):
        name = "Google"

        def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
            return "https://www.google.com"


    class MyFirstOperator(BaseOperator):

        operator_extra_links = (GoogleLink(),)

        def __init__(self, **kwargs):
            super().__init__(**kwargs)

        def execute(self, context):
            self.log.info("Hello World!")


    # Defining the plugin class
    class AirflowExtraLinkPlugin(AirflowPlugin):
        name = "extra_link_plugin"
        operator_extra_links = [
            GoogleLink(),
        ]

.. note:: Operator Extra Links should be registered via Airflow Plugins or custom Airflow Provider to work.

You can also add a global operator extra link that will be available to
all the operators through an Airflow plugin or through Airflow providers. You can learn more about it in the
:ref:`plugin interface <plugins-interface>` and in :doc:`apache-airflow-providers:index`.

You can see all the extra links available via community-managed providers in
:doc:`apache-airflow-providers:core-extensions/extra-links`.


Add or override Links to Existing Operators
-------------------------------------------

You can also add (or override) an extra link to an existing operators
through an Airflow plugin or custom provider.

For example, the following Airflow plugin will add an Operator Link on all
tasks using :class:`~airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSToS3Operator` operator.

**Adding Operator Links to Existing Operators**
``plugins/extra_link.py``:

.. code-block:: python

  from airflow.plugins_manager import AirflowPlugin
  from airflow.models.baseoperator import BaseOperatorLink
  from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator


  class S3LogLink(BaseOperatorLink):
      name = "S3"

      # Add list of all the operators to which you want to add this OperatorLinks
      # Example: operators = [GCSToS3Operator, GCSToBigQueryOperator]
      operators = [GCSToS3Operator]

      def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
          return "https://s3.amazonaws.com/airflow-logs/{dag_id}/{task_id}/{run_id}".format(
              dag_id=operator.dag_id,
              task_id=operator.task_id,
              run_id=ti_key.run_id,
          )


  # Defining the plugin class
  class AirflowExtraLinkPlugin(AirflowPlugin):
      name = "extra_link_plugin"
      operator_extra_links = [
          S3LogLink(),
      ]



**Overriding Operator Links of Existing Operators**:

It is also possible to replace a built in link on an operator via a Plugin. For example
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator` includes a link to the Google Cloud
Console, but if we wanted to change that link we could:

.. code-block:: python

    from airflow.plugins_manager import AirflowPlugin
    from airflow.models.baseoperator import BaseOperatorLink
    from airflow.models.xcom import XCom
    from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator

    # Change from https to http just to display the override
    BIGQUERY_JOB_DETAILS_LINK_FMT = "http://console.cloud.google.com/bigquery?j={job_id}"


    class BigQueryConsoleLink(BaseOperatorLink):
        """
        Helper class for constructing BigQuery link.
        """

        name = "BigQuery Console"
        operators = [BigQueryOperator]

        def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
            job_id = XCom.get_one(ti_key=ti_key, key="job_id")
            return BIGQUERY_JOB_DETAILS_LINK_FMT.format(job_id=job_id) if job_id else ""


    # Defining the plugin class
    class AirflowExtraLinkPlugin(AirflowPlugin):
        name = "extra_link_plugin"
        operator_extra_links = [
            BigQueryConsoleLink(),
        ]


**Adding Operator Links via Providers**

As explained in :doc:`apache-airflow-providers:index`, when you create your own Airflow Provider, you can
specify the list of operators that provide extra link capability. This happens by including the operator
class name in the ``provider-info`` information stored in your Provider's package meta-data:

Example meta-data required in your provider-info dictionary (this is part of the meta-data returned
by ``apache-airflow-providers-google`` provider currently:

.. code-block:: yaml

    extra-links:
      - airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleLink
      - airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink
      - airflow.providers.google.cloud.operators.mlengine.AIPlatformConsoleLink


You can include as many operators with extra links as you want.
