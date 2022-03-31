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



.. _howto/connection:cloudera:

Cloudera Data Engineering Connection
====================================

The Cloudera Data Engineering connection type enables integrations with Cloudera Data Engineering.

Authenticating to Cloudera Data Engineering
-------------------------------------------

Cloudera Data Engineering relies on CDP Authentication mechanisms.
The Cloudera Data Engineering connection relies currently only on CDP Access/Private Key pair,
which can be set up in the user's profile as described here:

https://docs.cloudera.com/cdp/latest/cli/topics/mc-cli-generating-an-api-access-key.html


Default Connection IDs
----------------------

Hooks and operators related to Cloudera Data Engineering use ``cde_runtime_api`` by default.

Configuring the Connection
--------------------------

Host (required)
   Specify the Virtual Cluster Jobs Api URL that can be obtained with the following steps:
     - From the CDE home page, go to Overview > Virtual Clusters > Cluster Details of the Virtual Cluster (VC) where you want the CDE job to run.
     - Click JOBS API URL to copy the URL.

CDP Access Key (required)
   Provide a CDP access key of the account for running jobs on the CDE VC.

CDP Private Key (required)
   Provide the CDP private key associated to the given CDP Access Key.

Extra (optional)
    Specify the extra parameter (as json dictionary) that can be used in the Cloudera Data Engineering connection.

    * ``cache_dir``: if for some reason the default cache_directory cannot be used because of insufficient access rights
