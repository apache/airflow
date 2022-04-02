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



Delta Sharing Sensor
====================

Use the :class:`~airflow.providers.delta.sharing.sensors.delta_sharing.DeltaSharingSensor` to wait for changes in a
given Delta Sharing table.

Using the Sensor
----------------

The sensor waits for version changes in a specified Delta Sharing table.

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
   * - delta_sharing_conn_id: str
     - name of the Delta Sharing connection that will be used to perform check.   By default and in the common case this will be ``delta_sharing_default``. To use token based authentication, provide the bearer token in the password field for the connection and put the base URL in the ``host`` field.
   * - profile_file: str
     - Optional path or HTTP(S) URL to a Delta Sharing profile file.  If this parameter is specified, the ``delta_sharing_conn_id`` isn't used.
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

Waiting for new version of Delta Sharing table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DeltaSharingSensor to wait for changes in Delta Sharing a table is as follows:

.. exampleinclude:: /../../airflow/providers/delta/sharing/example_dags/example_delta_sharing.py
    :language: python
    :start-after: [START howto_delta_sharing_sensor]
    :end-before: [END howto_delta_sharing_sensor]
