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

Supported versions
------------------

Version Life Cycle
``````````````````

Apache Airflow® version life cycle:

 .. This table is automatically updated by pre-commit scripts/ci/pre_commit/supported_versions.py
 .. Beginning of auto-generated table

=========  =====================  =========  ===============  =================  ================
Version    Current Patch/Minor    State      First Release    Limited Support    EOL/Terminated
=========  =====================  =========  ===============  =================  ================
2          2.10.2                 Supported  Dec 17, 2020     TBD                TBD
1.10       1.10.15                EOL        Aug 27, 2018     Dec 17, 2020       June 17, 2021
1.9        1.9.0                  EOL        Jan 03, 2018     Aug 27, 2018       Aug 27, 2018
1.8        1.8.2                  EOL        Mar 19, 2017     Jan 03, 2018       Jan 03, 2018
1.7        1.7.1.2                EOL        Mar 28, 2016     Mar 19, 2017       Mar 19, 2017
=========  =====================  =========  ===============  =================  ================

 .. End of auto-generated table


Limited support versions will be supported with security and critical bug fix only.
EOL versions will not get any fixes nor support.
We **highly** recommend installing the latest Airflow release which has richer features.


Support for Python and Kubernetes versions
``````````````````````````````````````````

As of Airflow 2.0 we agreed to certain rules we follow for Python and Kubernetes support.
They are based on the official release schedule of Python and Kubernetes, nicely summarized in the
`Python Developer's Guide <https://devguide.python.org/#status-of-python-branches>`_ and
`Kubernetes version skew policy <https://kubernetes.io/docs/setup/release/version-skew-policy>`_.

1. We drop support for Python and Kubernetes versions when they reach EOL. We drop support for those
   EOL versions in main right after EOL date, and it is effectively removed when we release the
   first new MINOR (Or MAJOR if there is no new MINOR version) of Airflow
   For example for Python 3.6 it means that we drop support in main right after 23.12.2021, and the first
   MAJOR or MINOR version of Airflow released after will not have it.

2. The "oldest" supported version of Python/Kubernetes is the default one. "Default" is only meaningful
   in terms of "smoke tests" in CI PRs which are run using this default version and default reference
   image available in DockerHub. Currently the ``apache/airflow:latest`` and ``apache/airflow:2.5.2`` images
   are Python 3.8 images, however, in the first MINOR/MAJOR release of Airflow released after 14.09.2023,
   they will become Python 3.9 images.

3. We support a new version of Python/Kubernetes in main after they are officially released, as soon as we
   make them work in our CI pipeline (which might not be immediate due to dependencies catching up with
   new versions of Python mostly) we release a new images/support in Airflow based on the working CI setup.
