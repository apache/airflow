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

airflowctl 0.1.2 (2026-02-20)
-----------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

- Add XCom CLI commands to airflowctl (#61021)
- Add auth list-envs command to list CLI environments and their auth status (#61426)
- Add allowed_run_types to whitelist specific dag run types (#61833)
- Default logical_date to now in airflowctl dagrun trigger to match UI behavior (#61047)

Bug Fixes
^^^^^^^^^

- Allow listing dag runs without specifying dag_id (#61525)
- Fix infinite password retry loop in airflowctl EncryptedKeyring initialization (#61329)
- Fix airflowctl auth login reporting success when keyring backend is unavailable (#61296)
- Fix airflowctl crash when incorrect keyring password is entered (#61042)
- Strip api-url for airflowctl auth login which fails with trailing slash (#61245)
- Fix airflow-ctl-tests files not triggering pre-commit integration tests (#61023)

Improvements
^^^^^^^^^^^^

- Print debug mode warning to stderr to avoid polluting stdout JSON output (#61302)
- Refactor datamodel defaulting logic into dedicated method (#61236)
- Alias run_after for XComResponse (#61443)
- Add test for sensitive config masking in airflowctl (#60361)

Miscellaneous
^^^^^^^^^^^^^

- Update keyring>=25.7.0 (#61529)
- Upgrade fastapi and conform openapi schema changes (#61476)
- Use SQLA's native Uuid/JSON instead of sqlalchemy-utils' types (#61532)
- Fix slots negative infinity (#61140)
- Pool API improve slots validation (#61071)
- Add ``team_name`` to Pool APIs (#60952)
- Add partition_key to DagRunAssetReference (#61725)
- Promote release_notes.rst into documentation that replace changelog.rst (#60482)
- Add HITLDetailHistory UI (#56760)
- Add static checker for preventing to increase dag version (#59430)


airflowctl 0.1.1 (2026-01-09)
-----------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

- Make pause/unpause commands positional for improved CLI consistency (#59936)
- Remove deprecated export functionality from airflowctl (#59850)
- Add ``team_name`` to connection commands (#59336)
- Add ``team_id`` to variable commands (#57102)
- Add pre-commit checks for airflowctl test coverage (#58856)
- Display active DAG run count in header with auto-refresh support (#58332)

Bug Fixes
^^^^^^^^^

- Simplify airflowctl exception handling in ``safe_call_command`` (#59808)
- Fix ``backfill`` default behavior for ``run_on_latest_version`` (#59304)
- Update ``BulkDeleteAction`` to use generic typing (#59207)
- Bump minimum supported ``prek`` version to 0.2.0 (#58952)
- Fix RST formatting to ensure blank lines before bullet lists (#58760)
- Update Python compatibility requirements and airflowctl documentation (#58653)
- Consistently exclude unsupported Python 3.14 (#58657)
- Improve cross-distribution dependency management (#58430)
- Synchronize documentation between official and convenience source installs (#58379)
- Add retry multiplier support (#56866)
- Fix documentation issues for installing from source distributions (#58366)
- Update ``pyproject.toml`` files to support ``pytest>=9.0.0`` TOML syntax (#58182)



airflowctl 0.1.0 (2025-11-05)
-----------------------------

Release of airflowctl, a command-line tool. There are lots of great features to use from start.
Please check the documentation for quick start and usage instructions.

Please visit quick start guide: :doc:`/start`

A new way of using Apache Airflow using CLI. Enhanced security is provided by using the Apache Airflow API to provide similar functionality to the Apache Airflow CLI.
Integrated with Keyring to enhance password security.
