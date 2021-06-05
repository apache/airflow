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

Tracking User Activity
======================

You can configure Airflow to route anonymous data to
`Apache Flagon UserALE <http://flagon.apache.org/userale/>`_, `Google Analytics <https://analytics.google.com/>`_,
`Metarouter <https://www.metarouter.io/>`_ or `Segment <https://segment.com/>`_.

Edit ``airflow.cfg`` and set the ``webserver`` block to have an ``analytics_tool`` and ``analytics_id``:

.. code-block:: ini

  [webserver]
  # Send anonymous user activity to your analytics tool
  # choose from apache_flagon_useralejs, google_analytics,
  # metarouter or segment.
  analytics_tool = google_analytics
  analytics_id = XXXXXXXXXXX

Additional configuration can be set for the ``apache_flagon_useralejs`` backend as follows

.. code-block:: ini

  [webserver]
  # Complete list of useralejs configuration can be found at
  # https://github.com/apache/incubator-flagon-useralejs#configure
  analytics_tool = apache_flagon_useralejs
  apache_flagon_useralejs_data_auth = ${ELASTICSEARCH_AUTH} or None
  apache_flagon_useralejs_data_autostart = True
  apache_flagon_useralejs_data_interval = 5000
  apache_flagon_useralejs_data_log_details = False
  apache_flagon_useralejs_data_resolution = 500
  apache_flagon_useralejs_data_threshold = 5
  apache_flagon_useralejs_data_tool = Apache Airflow GUI
  apache_flagon_useralejs_data_user = ${SPECIFIC_USERID}
  apache_flagon_useralejs_data_url = https://your.elasticsearch.host:port
  apache_flagon_useralejs_data_version = 2.1.0

.. note:: You can see view injected tracker html within Airflow's source code at
  ``airflow/www/templates/airflow/main.html``. The related global
  variables are set in ``airflow/www/app.py``.

.. note::
    For more information on setting the configuration, see :doc:`../howto/set-config`
