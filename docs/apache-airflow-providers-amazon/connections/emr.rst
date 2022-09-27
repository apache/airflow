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

.. _howto/connection:emr:

Amazon Elastic MapReduce (EMR) Connection
=========================================

.. note::
  This connection type only use for store parameters for Start EMR Cluster.

  This connection not intend to store any credentials for ``boto3`` client, if you try to pass any
  parameters not listed in `RunJobFlow API <https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html>`_
  you will get an error like this.

  .. code-block:: text

      Parameter validation failed: Unknown parameter in input: "region_name", must be one of:

  For Authenticating to AWS please use :ref:`Amazon Web Services Connection <howto/connection:aws>`.

Configuring the Connection
--------------------------

Extra (optional)
    Specify the parameters (as json dictionary) that can be used as initial configuration
    in :meth:`airflow.providers.amazon.aws.hooks.emr.EmrHook.create_job_flow` for propagate to
    `RunJobFlow API <https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html>`_.
    All parameters are optional.
