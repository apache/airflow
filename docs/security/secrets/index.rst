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

Secrets
=======

During Airflow operation, variables or configurations are used that contain particularly sensitive information.
This guide provides ways to protect this data.

The following are particularly protected:

* Variables. See the :ref:`Variables Concepts <concepts:variables>` documentation for more information.
* Connections. See the :ref:`Connections Concepts <concepts-connections>` documentation for more information.


.. toctree::
    :maxdepth: 1
    :glob:

    fernet
    secrets-backend/index
