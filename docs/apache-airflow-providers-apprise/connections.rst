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

.. _howto/connection:apprise:

Apprise Connection
=======================

The Apprise connection type enables connection to multiple services to send notifications. The complete list of services supported can be found  on the `Apprise Wiki <https://github.com/caronc/apprise/wiki#notification-services>`_.

Default Connection IDs
----------------------

Apprise hooks point to ``apprise_default`` by default.

Configuring the Connection
--------------------------
config (required)
    The service(s) to send notifications can be specified here. The format to specify single or multiple services is as follows::

    .. code-block:: python

       # Single service
       {"path": "URI for the service", "tag": "tag name"}

       # Multiple services
       [{"path": "URI for the service 1", "tag": "tag name"}, {"path": "URI for the service 2", "tag": "tag name"}]

    Read more about `path <https://github.com/caronc/apprise/wiki/URLBasics#apprise-url-basics>`_ and `tag <https://github.com/caronc/apprise/wiki/Development_API#tagging>`_
