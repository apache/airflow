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

Bundles
#######

Use the :class:`~airflow.providers.git.bundles.git.GitDagBundle` to configure a Git bundle in your Airflow's
``[dag_processor] dag_bundle_config_list``.

Example of using the GitDagBundle:

**JSON format example**:

.. code-block:: bash

    export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
     {
         "name": "my-git-repo",
         "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
         "kwargs": {
             "subdir": "dags",
             "tracking_ref": "main",
             "refresh_interval": 3600
             "submodules": False,
             "prune_dotgit_folder": True
         }
     }
    ]'
