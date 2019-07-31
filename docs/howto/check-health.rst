..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Checking Airflow Health Status
==============================

To check the health status of your Airflow instance, you can simply access the endpoint
``/health``. It will return a JSON object in which a high-level glance is provided.

.. code-block:: JSON

  {
    "metadatabase":{
      "status":"healthy"
    },
    "scheduler":{
      "status":"healthy",
      "latest_scheduler_heartbeat":"2018-12-26 17:15:11+00:00"
    }
  }

* The ``status`` of each component can be either "healthy" or "unhealthy"

  * The status of ``metadatabase`` depends on whether a valid connection can be initiated with the database

  * The status of ``scheduler`` depends on when the latest scheduler heartbeat was received

    * If the last heartbeat was received more than 30 seconds (default value) earlier than the current time, the scheduler is
      considered unhealthy
    * This threshold value can be specified using the option ``scheduler_health_check_threshold`` within the
      ``scheduler`` section in ``airflow.cfg``

Please keep in mind that the HTTP response code of ``/health`` endpoint **should not** be used to determine the health
status of the application. The return code is only indicative of the state of the rest call (200 for success).

