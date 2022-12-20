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

.. _howto/operator:SASStudioFlowOperator:

SASStudioFlowOperator
=======================
airflow.providers.sas.operators.sas_studioflow
The purpose of SAS Studio Flow Operator is to allow executing a SAS Studio Flow. It has these current capabilities:
 * Execute Studio Flow stored either on File System or in SAS Content
 * Select Compute Context to be used for execution of Studio Flow
 * Specify whether SAS logs of Studio Flow execution should be returned and displayed in Airflow or not
 * Specify parameters (init_code, wrap_code) to be used for code generation
 * Honor return code of SAS Studio Flow in Airflow. In particular, if SAS Studio Flow fails, Airflow raises exception as well and stop execution
 * Authentication via either OAuth token or login and password

Use the :class:`~airflow.providers.sas.operators.sas_studioflow` to execute SAS Studio Flows.

Common Usage
------------

To use the studio flow operator, three parameters are required: ``airflow_connection_name``, ``flow_path_type`` and ``flow_path``.

The ``airflow_connection_name`` parameter must be the name of an Airflow connection. You must create an HTTP connection in Airflow, either via the UI or through programmatic means. The connection should have a host name specified which should be in the form ``https://<hostname>[:port]``. You can specify a logon and password as well. If you'd rather specify an OAuth token you can leave the logon and password empty and specify the OAuth token in the extra field. It should be in json format with one element called token for example: ``{"token":"token_here"}``.

The ``flow_path_type`` parameter should be either 'content' or 'compute'. Most commonly, you will specify 'content'. In this case the ``flow_path`` parameter will reflect a path to the flow, for example ``/Public/Airflow/flowName.flw``. If you specify 'compute' then the ``flow_path`` parameter should be a full path to a volume mount that is accessible from the Compute session.

There are additional optional parameters:
  - flow_exec_log: True or False. Whether to output the execution log to the Airflow log.
  - flow_code_gen_init_code: True or False (default). Whether or not to generate standard init code
  - flow_code_gen_wrap_code: True or False (default) Whether or not to generate standard wrapper code
  - compute_context: string value. Name of the compute context to use. If not provided, a suitable default is used.
  - env_vars: Dictionary of environment variables to set before running the flow.

Example
-------

The code snippets below are based on Airflow-2.6

An example usage of the SASStudioFlowOperator is as follows:

.. exampleinclude:: /../../tests/system/providers/sas/example_sas_studioflow.py
    :language: python
    :start-after: [START sas_sf_howto_operator]
    :end-before: [END sas_sf_howto_operator]


The complete Studio Flow Operator DAG
---------------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../tests/system/providers/sas/example_sas_studioflow.py
    :language: python
    :start-after: [START sas_sf_howto]
    :end-before: [END sas_sf_howto]
