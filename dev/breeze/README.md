<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Apache Airflow Breeze](#apache-airflow-breeze)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

Apache Airflow Breeze
------------------------

The project is part of [Apache Airflow](https://airflow.apache.org) - it's a development environment
that is used by Airflow developers to effortlessly setup and maintain consistent development environment
for Airflow Development.

This package should never be installed in "production" mode. The `breeze` entrypoint will actually
fail if you do so. It is supposed to be installed only in [editable/development mode](https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/#working-in-development-mode)
directly from Airflow sources using `pipx` - usually with `--force` flag to account for re-installation
that might often be needed if dependencies change during development.

```shell
pipx install -e ./dev/breeze --force
```

You can read more about Breeze in the [documentation](https://github.com/apache/airflow/blob/main/BREEZE.rst)

This README file contains automatically generated hash of the `setup.py` and `setup.cfg` files that were
available when the package was installed. Since this file becomes part of the installed package, it helps
to detect automatically if any of the files have changed. If they did, the user will be warned to upgrade
their installations.

PLEASE DO NOT MODIFY THE HASH BELOW! IT IS AUTOMATICALLY UPDATED BY PRE-COMMIT.

---------------------------------------------------------------------------------------------------------

Package config hash: f28f0d555b81a0f48d6b29b3cf8bba132b8c6a8f3d290a25ad4fd62019a9adbf86c0dc913c474e23ae110f3f433db0214bf46b21000f0d2bdd0884134923ae91

---------------------------------------------------------------------------------------------------------
