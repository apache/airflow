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


REST API Documentation
======================

Airflow has a REST API that allows third-party application to perform a wide wide range of operations.

Page size limit
---------------

To protect against requests that may lead to application instability, the API has a limit of items in response.
The default is 100 items, but you can change it using ``maximum_page_limit``  option in ``[api]``
section in the ``airflow.cfg`` file.

.. note::
    For more information on setting the configuration, see :doc:`../howto/set-config`
