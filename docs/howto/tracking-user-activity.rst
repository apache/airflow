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
=============================

You can configure Airflow to route anonymous data to
`Google Analytics <https://analytics.google.com/>`_,
`Segment <https://segment.com/>`_, or `Metarouter <https://www.metarouter.io/>`_.

Edit ``airflow.cfg`` and set the ``webserver`` block to have an ``analytics_tool`` and ``analytics_id``:

.. code-block:: python

  [webserver]
  # Send anonymous user activity to Google Analytics, Segment, or Metarouter
  analytics_tool = google_analytics # valid options: google_analytics, segment, metarouter
  analytics_id = XXXXXXXXXXX

.. note:: You can see view injected tracker html within Airflow's source code at
  ``airflow/www_rbac/templates/appbuilder/baselayout.html``. The related global
  variables are set in ``airflow/www_rbac/templates/app.py``.
