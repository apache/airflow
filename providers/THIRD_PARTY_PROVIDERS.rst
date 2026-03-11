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

3rd-Party Providers
====================

.. contents:: Table of Contents
   :depth: 3
   :local:

Relation to community providers
---------------------------------

Providers, can also be maintained and released by 3rd parties (service providers or systems integrators).
There is no difference between the community and 3rd party providers - they have all the same capabilities
and limitations.

This is especially in case the provider concerns 3rd-party service that has a team that can manage provider
on their own, has a rapidly evolving live service, and believe they need a faster release cycle than the community
can provide.

Information about such 3rd-party providers are usually published at the
`Ecosystem: plugins and providers <https://airflow.apache.org/ecosystem/#third-party-airflow-plugins-and-providers>`_
page of the Airflow website and we encourage the service providers to publish their providers there. You can also
find a 3rd-party registries of such providers, that you can use if you search for existing providers (they
are also listed at the "Ecosystem" page in the same chapter)

While we already have - historically - a number of 3rd-party service providers managed by the community,
most of those services have dedicated teams that keep an eye on the community providers and not only take
active part in managing them (see mixed-governance model below), but also provide a way that we can
verify whether the provider works with the latest version of the service via dashboards that show
status of System Tests for the provider. This allows us to have a high level of confidence that when we
release the provider it works with the latest version of the service. System Tests are part of the Airflow
code, but they are executed and verified by those 3rd party service teams. We are working with the 3rd
party service teams (who are often important stakeholders of the Apache Airflow project) to add dashboards
for the historical providers that are managed by the community, and current set of Dashboards can be also
found at the
`Ecosystem: system test dashboards <https://airflow.apache.org/ecosystem/#airflow-provider-system-test-dashboards>`_
