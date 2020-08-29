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



Logging and Monitoring architecture
===================================

Airflow supports a variety of logging and monitoring mechanisms as shown below.

.. image:: ../img/arch-diag-logging.png

By default, Airflow supports logging into the local file system. These include logs from the Web server, the Scheduler, and the Workers running tasks. This is suitable for development environments and for quick debugging.

For cloud deployments, Airflow also has hooks contributed by the Community for logging to cloud storage such as AWS, Google Cloud, and Azure.

The logging settings and options can be specified in the Airflow Configuration file, which as usual needs to be available to all the Airflow process: Web server, Scheduler, and Workers.

For production deployments, we recommend using FluentD to capture logs and send it to destinations such as ElasticSearch or Splunk.

.. note::
    For more information on configuring logging, see :doc:`/logging-monitoring/logging-tasks`

Similarly, we recommend using StatsD for gathering metrics from Airflow and send them to destinations such as Prometheus.

.. note::
    For more information on configuring metrics, see :doc:`/logging-monitoring/metrics`
