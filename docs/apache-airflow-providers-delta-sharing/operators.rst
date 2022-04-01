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


.. _howto/operator:DeltaSharingOperators:


DeltaSharingDownloadToLocalOperator
===================================

Use the :class:`~airflow.providers.delta.sharing.operators.delta_sharing.DeltaSharingLocalDownloadOperator` to
download data from a given Delta Sharing table to a local disk.


Using the Operator
------------------

Operator downloads data to a local disk from a specified Delta Sharing table.

.. list-table::
   :widths: 15 25
   :header-rows: 1

   * - Parameter
     - Input
   * - share: str
     - name of the share in which check will be performed
   * - schema: str
     - name of the schema (database) in which check will be performed
   * - table: str
     - name of the table to check
   * - location: str
     - name of directory where downloaded data will be stored. This field will be templated.
   * - predicates: list
     - optional list of strings that will be ANDed to build a filter expression. This field will be templated.
   * - limit: int
     - optional limit on the number of records to return.
   * - save_partitioned: bool
     - if ``True`` (default), data will be saved by partitions. if ``False``, all data files will be  saved into a top-level directory.
   * - save_metadata: bool
     - if ``True`` (default), metadata will be shared into a ``_metadata/<version>.json`` file
   * - save_stats: bool
     - if ``True`` (default), per-file statistics will be saved into a ``_stats/<data_file_name>.json``
   * - overwrite_existing: bool
     - If ``False`` (default), file with the same name exists and has the same size as returned in file metadata, then it won't be re-downloaded and overwritten.
   * - num_parallel_downloads: int
     - number of parallel downloads. Default is 5.
   * - delta_sharing_conn_id: str
     - name of the Delta Sharing connection that will be used to perform check.   By default and in the common case this will be ``delta_sharing_default``. To use token based authentication, provide the bearer token in the password field for the connection and put the base URL in the ``host`` field.
   * - timeout_seconds: int
     - The timeout for this run. By default a value of 0 is used which means to have no timeout.
   * - retry_limit: int
     - Amount of times retry if the Delta Sharing backend is  unreachable. Its value must be greater than or equal to 1. Default is 3.
   * - retry_delay: float
     - Number of seconds to initial wait between retries (it might be a floating point number).
   * - retry_args: dict
     - An optional dictionary with arguments passed to ``tenacity.Retrying`` class.

Examples
--------

Downloading data from Delta Sharing table to a local disk
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DeltaSharingLocalDownloadOperator to download data from a  Delta Sharing table is as follows:

.. exampleinclude:: /../../airflow/providers/delta/sharing/example_dags/example_delta_sharing.py
    :language: python
    :start-after: [START howto_delta_sharing_operator]
    :end-before: [END howto_delta_sharing_operator]
