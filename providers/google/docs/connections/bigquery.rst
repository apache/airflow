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



.. _howto/connection:gcpbigquery:

Google Cloud BigQuery Connection
================================

The Google Cloud BigQuery connection type enables integration with the Google Cloud BigQuery.
As it is built on the top of Google Cloud Connection (i.e., BigQuery hook inherits from
GCP base hook), the basic authentication methods and parameters are exactly the same as the Google Cloud Connection.
Extra parameters that are specific to BigQuery will be covered in this document.


Configuring the Connection
--------------------------
.. note::
  Please refer to :ref:`Google Cloud Connection docs<howto/connection:gcp:configuring_the_connection>`
  for information regarding the basic authentication parameters.

Impersonation Scopes


Use Legacy SQL
  Whether or not the connection should utilize legacy SQL.

Location
    One of `BigQuery locations <https://cloud.google.com/bigquery/docs/locations>`_ where the dataset resides.
    If None, it utilizes the default location configured in the BigQuery service.

Priority
    Should be either "INTERACTIVE" or "BATCH",
    see `running queries docs <https://cloud.google.com/bigquery/docs/running-queries>`_.
    Interactive query jobs, which are jobs that BigQuery runs on demand.
    Batch query jobs, which are jobs that BigQuery waits to run until idle compute resources are available.

API Resource Configs
    A dictionary containing parameters for configuring the Google BigQuery Jobs API.
    These configurations are applied according to the specifications outlined in the
    `BigQuery Jobs API documentation <https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs>`_.
    For example, you can specify configurations such as {'query': {'useQueryCache': False}}.
    This parameter is useful when you need to provide additional parameters that are not directly supported by the
    BigQueryHook.

Labels
    A dictionary of labels to be applied on the BigQuery job.
