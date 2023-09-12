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

This page contains the list of all the available Airflow configurations that you
can set in ``airflow.cfg`` file or using environment variables.

Use the same configuration across all the Airflow components. While each component
does not require all, some configurations need to be same otherwise they would not
work as expected. A good example for that is :ref:`secret_key<config:webserver__secret_key>` which
should be same on the Webserver and Worker to allow Webserver to fetch logs from Worker.

The webserver key is also used to authorize requests to Celery workers when logs are retrieved. The token
generated using the secret key has a short expiry time though - make sure that time on ALL the machines
that you run airflow components on is synchronized (for example using ntpd) otherwise you might get
"forbidden" errors when the logs are accessed.

Some of the providers have their own configuration options, you will find details of their configuration
in the provider's documentation. The pre-installed providers that you may want to configure are:

* :doc:`Configuration Reference for Celery Provider <apache-airflow-providers-celery:configurations-ref>`
* :doc:`Configuration Reference for Apache Hive Provider <apache-airflow-providers-apache-hive:configurations-ref>`
* :doc:`Configuration Reference for CNCF Kubernetes Provider <apache-airflow-providers-cncf-kubernetes:configurations-ref>`
* :doc:`Configuration Reference for SMTP Provider <apache-airflow-providers-smtp:configurations-ref>`
* :doc:`Configuration Reference for IMAP Provider <apache-airflow-providers-imap:configurations-ref>`
* :doc:`Configuration Reference for OpenLineage Provider <apache-airflow-providers-openlineage:configurations-ref>`
* :doc:`Configuration Reference for Elasticsearch Provider <apache-airflow-providers-elasticsearch:configurations-ref>`
* :doc:`Configuration Reference for Amazon Provider <apache-airflow-providers-amazon:configurations-ref>`

.. note::
    For more information see :doc:`/howto/set-config`.

.. include:: ../exts/includes/sections-and-options.rst
