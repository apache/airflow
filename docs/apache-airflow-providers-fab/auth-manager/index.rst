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

Flask AppBuilder (FAB) auth manager
===================================

.. note::
    Before reading this, you should be familiar with the concept of auth manager.
    See :doc:`apache-airflow:core-concepts/auth-manager/index`.

FAB auth (for authentication/authorization) manager is the auth manager that comes by default with Airflow. This auth manager defines the user authentication and user authorization by default in Airflow.
The backend used to store all entities used by the FAB auth manager is the Airflow database: :doc:`apache-airflow:database-erd-ref`.

.. image:: ../img/diagram_fab_auth_manager_airflow_architecture.png

Follow the below topics as well to understand other aspects of FAB auth manager:

.. toctree::
    :maxdepth: 1
    :glob:

    *
