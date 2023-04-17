
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


Using OpenLineage integration
-----------------------------

Install
=======

To use OpenLineage

Config
======

Primary method of configuring OpenLineage Airflow Provider is Airflow configuration.

One thing that needs to be set up in every case is ``Transport`` - where do you wish for
your events to end up.

Another option of configuration is using ``openlineage.yml`` file.
Detailed description of that configuration method is in OpenLineage docs
https://openlineage.io/docs/client/python#configuration
