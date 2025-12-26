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


Adding Connections, Variables and Environment Variables
=======================================================

You can programmatically add Connections, Variables and arbitrary Environment Variables to your
Airflow deployment using the Helm chart.


Connections and Sensitive Environment Variables
-----------------------------------------------
Under the ``secret`` and ``extraSecret`` sections of the ``values.yaml`` you can pass connection strings and sensitive
environment variables into Airflow using the Helm chart. To illustrate, lets create a yaml file called ``override.yaml``
to override values under these sections of the ``values.yaml`` file.

.. code-block:: yaml

   # override.yaml

   secret:
     - envName: "AIRFLOW_CONN_GCP"
       secretName: "my-airflow-connections"
       secretKey: "AIRFLOW_CONN_GCP"
     - envName: "my-env"
       secretName: "my-secret-name"
       secretKey: "my-secret-key"

   extraSecrets:
     my-airflow-connections:
       data: |
         AIRFLOW_CONN_GCP: 'base64_encoded_gcp_conn_string'
     my-secret-name:
       stringData: |
         my-secret-key: my-secret


Variables
---------
Airflow supports Variables which enable users to craft dynamic Dags. You can set Variables in Airflow in three ways - UI,
command line, and within your Dag file. See :doc:`apache-airflow:howto/variable` for more.

With the Helm chart, you can also inject environment variables into Airflow. So in the example ``override.yaml`` file,
we can override values of interest in the ``env`` section of the ``values.yaml`` file.

.. code-block:: yaml

   env:
     - name: "AIRFLOW_VAR_KEY"
       value: "value_1"
     - name: "AIRFLOW_VAR_ANOTHER_KEY"
       value: "value_2"


You can also utilize ``extraEnv`` and ``extraEnvFrom`` if you need the name or value to be templated.

.. code-block:: yaml

   extraEnv: |
     - name: AIRFLOW_VAR_HELM_RELEASE_NAME
       value: '{{ .Release.Name }}'

   extraEnvFrom: |
     - configMapRef:
         name: '{{ .Release.Name }}-airflow-variables'

   extraConfigMaps:
     '{{ .Release.Name }}-airflow-variables':
       data: |
         AIRFLOW_VAR_HELLO_MESSAGE: "Hi!"
