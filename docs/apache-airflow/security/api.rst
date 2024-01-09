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

API
===

API Authentication
------------------

The API authentication is handled by the auth manager. For more information about API authentication, please refer to the auth manager documentation used by your environment.
By default Airflow uses the FAB auth manager, if you did not specify any other auth manager, please look at :doc:`apache-airflow-providers-fab:auth-manager/api-authentication`.

Enabling CORS
-------------

`Cross-origin resource sharing (CORS) <https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS>`_
is a browser security feature that restricts HTTP requests that are initiated
from scripts running in the browser.

``Access-Control-Allow-Headers``, ``Access-Control-Allow-Methods``, and
``Access-Control-Allow-Origin`` headers can be added by setting values for
``access_control_allow_headers``, ``access_control_allow_methods``, and
``access_control_allow_origins`` options in the ``[api]`` section of the
``airflow.cfg`` file.

.. code-block:: ini

    [api]
    access_control_allow_headers = origin, content-type, accept
    access_control_allow_methods = POST, GET, OPTIONS, DELETE
    access_control_allow_origins = https://exampleclientapp1.com https://exampleclientapp2.com

Page size limit
---------------

To protect against requests that may lead to application instability, the stable API has a limit of items in response.
The default is 100 items, but you can change it using ``maximum_page_limit``  option in ``[api]``
section in the ``airflow.cfg`` file.
