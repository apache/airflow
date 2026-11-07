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

.. This file is populated while releasing after cutting the release candidate. Please do not edit in PRs.

airflowctl 0.1.6 (2026-11-07)
-----------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

- Add ``dags state`` command (#68564, #69915)
- Add ``dagrun delete`` command (#68046)
- Add ``tasks states-for-dag-run``, ``tasks clear`` and ``taskinstances list`` commands. ``tasks clear``
  previews by default through ``dry_run`` and changes task instances only when that is turned off (#69366)
- Exit non-zero when ``connections test`` reports a failed test. It used to exit 0, so check scripts
  that rely on the old exit code (#76281)
- Print the status messages of ``dags pause`` and ``dags unpause`` to stderr. Scripts that read them from
  stdout must read stderr instead (#76133)
- Keep ``auth token`` stdout limited to the token (#76134)
- Keep stdout parseable when a request is retried or fails, by sending those messages to stderr (#76347)
- Retry writes only when the server could not have received them, and warn when a write may have
  succeeded despite an error (#74382)
- Raise the default API request timeout from 5 to 30 seconds, configurable with
  ``AIRFLOW_CLI_API_TIMEOUT`` (#76125)
- Require ``rich-argparse`` 1.4.0 or later (#76661)

Bug Fixes
^^^^^^^^^

- Stop sending the stored token to the URL given with ``--api-url`` in auth commands (#76280)
- Stop sending an old token to a new API URL (#76287)
- Stop update commands from clearing fields left unset (#71333)
- Fix ids with URL special characters being sent to the wrong path (#76147)
- Fix incorrect asset queued-event endpoint paths (#70461)
- Fix ``backfill`` pause, unpause and cancel always failing with 405 (#73282)
- Fix ``backfill create-dry-run`` printing one all-null row (#76224)
- Fix ``dagrun list`` ignoring ``--start-date`` and ``--end-date`` (#76348)
- Fix ``dags get-tags`` crashing whenever a Dag has tags (#73743)
- Fix ``pools`` export and import dropping pool team ownership (#73132)
- Fix ``connections test`` failing on stored connections (#73098)
- Fix ``connections create-defaults`` failing on a None response (#68853)
- Fix ``config lint`` warnings for ``remove_if_equals`` rules (#66370)
- Fix ``config lint`` filters matching partial config names (#75690)
- Fix ``variables import`` with structured falsy values (#67060)
- Fix JSON parsing of Dag run conf (#68985)
- Fix list operations ignoring the requested offset (#70989)
- Fix list pagination when the server caps the page size (#76268)
- Fix list argument parsing (#72454)
- Fix ``datetime`` parameters rejecting all input (#70249)
- Fix ``--env`` being ignored by non-auth commands (#73808)
- Fix operation defaults being lost when an optional flag is omitted (#65062)
- Fix the validation error of the operations create command (#69317)
- Fix commands crashing instead of printing their result (#72675)
- Fix enum names and non-ISO dates in ``dags`` output (#76354)
- Fix square brackets being dropped from table and plain output (#76175)
- Fix failures on fields added by newer API servers (#75610)
- Accept the ``operator_triggered`` Dag run type from Airflow 3.3+ servers (#63733)
- Accept the ``awaiting_input`` task instance state from Airflow 3.3+ servers (#76118)
- Accept Airflow 3.4 Dag warning types and plugin destinations (#76119)
- Accept null ``fileloc`` values on Dags (#69110)
- Accept null Variable values from servers that cannot decrypt them (#65452)
- Send a request body with ``assets materialize``, which Airflow 3.4 requires (#73328)
- Reject a non-positive ``max_active_runs`` in ``backfill create`` (#75071)
- Stop a bad retry environment variable from breaking every command (#72429)
- Stop swallowing unexpected keyring errors when saving credentials (#70991)
- Handle connection and request timeouts (#76132)
- Handle empty or corrupted JSON when loading credentials (#75279)
- Report an unusable credentials file without a traceback (#72468)
- Report the server error instead of a traceback on 5xx responses (#70841)
- Report redirects and non-JSON error bodies without a traceback (#73307)
- Report response validation errors without a traceback (#76121)
- Keep API client error details behind a reverse proxy (#75493)
- Report missing required options as readable errors (#75177)
- Report missing and invalid fields in ``connections import`` (#76090)
- Validate the file structure in ``connections import`` (#75176)
- Report invalid pools in ``pools import`` as readable errors (#76173)
- Report invalid variables in ``variables import`` as readable errors (#76174)
- Escape server and user text in command messages (#76264, #76265, #76419)
- Make command generation safe to read more than once (#73097)

Improvements
^^^^^^^^^^^^

- Allow a non-default API server URL through ``AIRFLOW_CLI_API_URL`` (#76124)
- Flag the deprecated ``expose_config=non-sensitive-only`` in ``config lint`` (#72507)
- Log the retry reason in API client retries (#75059)
- Drop the redundant re-validation round trip in list pagination (#71928)
- Simplify query parameter handling (#69475)
- Fix wrong command and API wording in docs and help (#75058)
- Fix typos, grammar and broken references in the docs (#73146)
- Limit the CLI reference's page contents to top-level commands (#75483)

Known Issues
^^^^^^^^^^^^

- ``dags update`` without ``--is-paused`` sends ``is_paused: false``, so it unpauses the Dag. Pass
  ``--is-paused`` to keep a paused Dag paused.
- ``pools update`` without ``--include-deferred`` sends ``include_deferred: false``, so it resets that
  setting. Pass ``--include-deferred`` to keep it on.

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
  - Remove asset state store commands from airflowctl 0.1.x, reverting #70771 and #75753 before release (#76662)
  - Fix airflowctl dags state crash from an incomplete backport (#71095)
  - Regenerate airflowctl datamodels from this branch's API spec (#71094)
  - Fix airflow-ctl mypy static check on the 0.1 branch (#71096)
  - Run CI on airflow-ctl release-branch pull requests and pushes (#70724, #70726)
  - Bump datamodel-code-generator pin from 0.33.0 to 0.41.0 (#69854)
  - Remove no-op exception re-raise and parser override (#70903)
  - Remove no-op ServerResponseError re-raise blocks in operations (#70376)
  - Remove unused airflowctl lowercase argument helper (#75768)
  - Remove unused type: ignore comments in airflow-ctl (#75203)
  - Fix airflowctl retry env-var tests failing on self-hosted runners (#73212)
  - Parametrize airflowctl environment output tests (#75900)
  - Stop airflowctl connection-refused test depending on the local network (#76077)
  - Add tests for airflowctl utils helpers and module loading (#76271)

airflowctl 0.1.5 (2026-05-26)
-----------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

- Add dags next execution command #66172 (#66188)
- Add bulk delete Dag Runs (#67095)
- Add ``rerun_with_latest_version`` config hierarchy for clear/rerun behavior (#63884)
- Implement patching of task group instances in API (#62812)
- Allow remote version check without authentication (#65099)
- Add cursor based pagination for get_dag_runs endpoint (#65604)
- Enable queue up new tasks (#63484)
- Add cursor based pagination for get_task_instances endpoint (#64845)
- Add ``is_backfillable`` property to DAG API responses (#64644)
- Expose required primitive parameters of auto-generated commands as positional
  arguments instead of ``--flag`` options. Optional parameters keep the
  ``--flag`` form. Follows the dev-list lazy consensus on airflowctl parameter
  style (see `<https://lists.apache.org/thread/m1qvcvow3l17ytv40vhslh40wn3rntrm>`_) (#66768)

Bug Fixes
^^^^^^^^^

- Fix connections import schema handling (#67063)
- Fix broken download URLs and variable names in docs (#67046)
- Fix  missing pyyaml runtime dependency (#65489)
- Fix dagrun list crash when --state is omitted (#65608)
- Fix backfill params not overriding existing DAG run conf (#64939)
- Fix ruff on client-py (#64868)

Improvements
^^^^^^^^^^^^

- AIP-103: Add Core API endpoints for task state and asset state (#67041)
- Comment to not edit RELEASE_NOTES.rst manually in PRs for airflowctl (#67128)
- Align Dag capitalization from "DAG" to "Dag" for airflow-ctl/ (#66112)
- Send backfill create and dry-run payloads as JSON (#65158)
- Use existing safe_load function in airflowctl utils to load help texts (#65841)
- Cap airflow-ctl httpx dependency below 1.0 (#65607)
- Remove dead airflow-ctl/newsfragments directory (unused by changelog tooling) (#65507)
- Incorrect fallback logic (#64586)
- Run non-provider mypy as regular prek static checks instead of separate CI jobs (#64780)
- Clear, Mark Success/Fail and delete multiple Task Instances (#64141)

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
  - Upgrade important CI environment (#67313)
  - Upgrade important CI environment (#66843)
  - Upgrade important CI environment (#66068)
  - Bump uv to 0.11.8 to adopt uv.lock-conflict fix (#66042)
  - Upgrade important CI environment (#65933)
  - Upgrade important CI environment (#65521)
  - Upgrade important CI environment (#65525)
  - CI: Upgrade important CI environment (#64458)
  - CI: Upgrade important CI environment (#64451)
  - Add 4-day cooldown for uv dependency resolution (#64249)
  - Upgrade important CI environment (#64239)

airflowctl 0.1.4 (2026-04-21)
-----------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

- Add YAML-based help texts for auto-generated airflowctl commands (#65073)
- Added plugins command to airflowctl (#64935)
- Python 3.14 support (#63520)

Bug Fixes
^^^^^^^^^

- Declare ``pyyaml`` as a runtime dependency so ``airflowctl`` starts without crashing on ``ModuleNotFoundError`` (#65489)
- Fix ``airflowctl dagrun list`` crash when ``--state`` is omitted (#65608)
- Prevent path traversal via ``AIRFLOW_CLI_ENVIRONMENT`` in airflowctl (#64618)
- Fix ``is_alive`` default in ``airflowctl jobs list`` to show all jobs (#65065)
- Fix CLI error handling and exit codes for failed commands (#65052)
- Fix ``airflowctl connections import`` to return non-zero exit code on failure (#64416)
- Fix ``airflowctl variables import`` to correctly handle falsy values (#64362)
- Fix ``airflowctl version`` command prompting for keyring credentials (#63772)
- Fix ``airflowctl`` boolean flags on Python 3.14 (#63587)
- Allow ``null`` ``dag_run_conf`` in ``BackfillResponse`` serialization (#63259)

Improvements
^^^^^^^^^^^^

- Use DAG form when materializing assets in airflowctl (#64211)
- Mention Python 3.14 support in docs (#63950)

Miscellaneous
^^^^^^^^^^^^^

- Cap ``httpx`` below 1.0 to avoid pulling the pre-release rewrite on ``--prerelease=allow`` installs (#65607)
- Run non-provider mypy checks as regular prek static checks (#64780)
- Add ``*.iml`` to ``.gitignore`` in all distributions (#63636)
- Sync airflowctl from main to v3-2-test (#65069)
- CI: Upgrade important CI environment (#64458, #65525)


airflowctl 0.1.3 (2026-03-09)
-----------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

- Add airflowctl auth token command to print JWT access tokens (#62843)
- Add ``--action-on-existing-key`` to ``pools import`` and ``connections import`` (#62702)
- Add retry mechanism to airflowctl and remove flaky integration mark (#63016)
- airflowctl auth login: prompt for credentials interactively when none are provided (#62549)
- feat(airflowctl): support on headless environments (#62217)

Bug Fixes
^^^^^^^^^

- Fix ``airflowctl pools export`` ignoring ``--output`` table/yaml/plain (#62665)
- Fix ``airflowctl connections import`` failure when JSON omits ``extra`` field (#62662)
- Amend compatibility issues for airflowctl (#63388)

Improvements
^^^^^^^^^^^^

- Send ``limit`` parameter in ``execute_list`` server requests (#63048)
- Run test coverage when airflowctl command has any change (#63216)
- airflow-ctl: add coverage tests for console formatting output (#62627)
- Clean up stale Python 3.9 workaround in airflow-ctl CLI config parser (#62206)
- Expose ``timetable_partitioned`` in UI API (#62777)

Miscellaneous
^^^^^^^^^^^^^

- CI: upgrade important CI environment (#62610)
- Fix all build-system requirements including transitive dependencies (#62570)
- Add DagRunType for asset materializations (#62276)


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
- Fix ``airflow-ctl-tests`` files not triggering pre-commit integration tests (#61023)

Improvements
^^^^^^^^^^^^

- Print debug mode warning to stderr to avoid polluting stdout JSON output (#61302)
- Refactor ``datamodel`` defaulting logic into dedicated method (#61236)
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
