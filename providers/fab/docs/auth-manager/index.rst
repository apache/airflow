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

FAB auth (for authentication/authorization) manager defines the user authentication and user authorization in Airflow.
The backend used to store all entities used by the FAB auth manager is the Airflow database: :doc:`apache-airflow:database-erd-ref`.

.. image:: ../img/diagram_fab_auth_manager_airflow_architecture.png

Follow the below topics as well to understand other aspects of FAB auth manager:

* :doc:`token`. Use FAB auth manager to generate a JWT token in order to call Airflow public API
* :doc:`api-authentication`. How to authenticate the API provided by FAB auth manager
* :doc:`access-control`. How FAB auth manager manage users and permissions
* :doc:`webserver-authentication`. To learn the different options available in FAB auth manager to authenticate users
* :doc:`security`. To learn the different options available in FAB auth manager to secure the UI provided by FAB auth manager
* :doc:`sso`. How to configure Single Sign-On (SSO) with OAuth2 providers using FAB auth manager
* :doc:`configuring-flask-app`. To learn the configuration options available for the flask app used by ``FabAuthManager``.

.. toctree::
    :hidden:
    :maxdepth: 1
    :glob:

    *
