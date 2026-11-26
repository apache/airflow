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

.. _fixing-image-at-release-time:

Image update policy
===================

The released "versioned" reference images are mostly ``fixed`` when we release Airflow version and we only
update them in exceptional circumstances. For example when we find out that there are dependency errors
that might prevent important Airflow or embedded provider's functionalities working. In normal circumstances,
the images are not going to change after release, even if new version of Airflow dependencies are released -
not even when those versions contain critical security fixes. The process of Airflow releases is designed
around upgrading dependencies automatically where applicable but only when we release a new version of Airflow,
not for already released versions.

Why security scanners report vulnerabilities
--------------------------------------------

We often hear questions that our users use various security scanners on the image and find out that
there are some critical and high vulnerabilities in the image - not coming from Airflow but for some other
components. In general, this is normal and expected that such vulnerabilities are found in the image after
it's been released and fixed - precisely because we are NOT updating the images after they are released as
explained above. Also sometimes even the latest releases contain vulnerabilities that are not yet fixed
in the base image we use or in the dependencies we use and cannot upgrade, because some of our providers
have limits and did not manage to upgrade yet and we have no control over that. So it is possible
that even the most recent release of our image there are some High and Critical vulnerabilities that
are not yet fixed.

See :doc:`security-scan-results` for what you can do about it.
