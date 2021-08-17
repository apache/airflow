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



To use these operators, you must do a few things:

  * Select or create a Cloud Platform project using the `Cloud Console <https://console.cloud.google.com/project>`__.
  * Enable billing for your project, as described in the `Google Cloud documentation <https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project>`__.
  * Enable the API, as described in the `Cloud Console documentation <https://cloud.google.com/apis/docs/enable-disable-apis>`__.
  * Install API libraries via **pip**.

    .. code-block:: bash

      pip install 'apache-airflow[google]'

    Detailed information is available for :doc:`Installation <apache-airflow:installation>`.

  * :doc:`Setup a Google Cloud Connection </connections/gcp>`.
