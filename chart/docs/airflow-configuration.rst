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

Configuring Airflow
-------------------

The chart allows for setting arbitrary Airflow configuration in values under the ``config`` key.
Some of the defaults in the chart differ from those of core Airflow and can be found in
`values.yaml <https://github.com/apache/airflow/blob/main/chart/values.yaml>`__.

As an example of setting arbitrary configuration, the following yaml demonstrates how one would
allow webserver users to view the config from within the UI:

.. code-block:: yaml

   config:
     api:
       expose_config: 'True'  # by default this is 'False'

Generally speaking, it is useful to familiarize oneself with the Airflow
configuration prior to installing and deploying the service.

.. note::

  The recommended way to load example Dags using the official Docker image and chart is to configure the ``AIRFLOW__CORE__LOAD_EXAMPLES`` environment variable
  in ``extraEnv`` (see :doc:`Parameters reference <parameters-ref>`). The official Docker image has ``AIRFLOW__CORE__LOAD_EXAMPLES=False``
  set within the image, so you need to override it with an environment variable when deploying the chart in order for the examples to be present.
