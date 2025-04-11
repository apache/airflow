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

Configuration Reference
.......................

.. jinja:: config_ctx

  This page contains the list of all available Airflow configurations for the
  ``{{ package_name }}`` provider that can be set in the ``airflow.cfg`` file or using environment variables.

  .. note::
    The configuration embedded in providers started to be used as of Airflow 2.7.0. Previously the
    configuration was described and configured in the Airflow core package - so if you are using Airflow
    below 2.7.0, look at Airflow documentation for the list of available configuration options
    that were available in Airflow core.

  .. note::
     For more information see :doc:`apache-airflow:howto/set-config`.
