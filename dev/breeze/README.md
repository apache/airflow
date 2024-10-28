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

NOTE! If you see below warning - it means that you hit [known issue](https://github.com/pypa/pipx/issues/1092)
with `packaging` version 23.2
⚠️ Ignoring --editable install option. pipx disallows it for anything but a local path,
to avoid having to create a new src/ directory.

The workaround is to downgrade packaging to 23.1 and re-running the `pipx install` command, for example
by running `pip install "packaging<23.2"`.

```shell
pip install "packaging<23.2"
pipx install -e ./dev/breeze --force
```


You can read more about Breeze in the [documentation](https://github.com/apache/airflow/blob/main/dev/breeze/doc/README.rst)

This README file contains automatically generated hash of the `pyproject.toml` files that were
available when the package was installed. Since this file becomes part of the installed package, it helps
to detect automatically if any of the files have changed. If they did, the user will be warned to upgrade
their installations.

PLEASE DO NOT MODIFY THE HASH BELOW! IT IS AUTOMATICALLY UPDATED BY PRE-COMMIT.

---------------------------------------------------------------------------------------------------------

Package config hash: f13c42703e0a262d9f3c1bee608ff32c368be4c6a11f150a2f95809938641f5ec07904d5cc2e3944dfe4d206dc52846f8b81193fc279a333ff898dd033e07be4

---------------------------------------------------------------------------------------------------------
