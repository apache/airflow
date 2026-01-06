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

``apache-airflow-providers-fab``

Changelog
---------

3.1.1
.....

Misc
~~~~

* ``Remove global from plugins_manager (#59851)``
* ``Replace imports from airflow.security.permissions module in fab provider due to future deprecation of the module. (#59755)``
* ``Split SerializedBaseOperator from serde logic (#59627)``
* ``Adaptions for custom auth manager example in documentation (#59355)``
* ``Pnpm upgrade to 10.x and prevent script execution (#59466)``
* ``Bump the fab-ui-package-updates group across 1 directory with 3 updates (#59242)``

Doc-only
~~~~~~~~

* ``Add hints for claim validation to auth manager documentation (#59320)``
* ``Fix release process issues after going through 2025-12-09 release (#59261)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.1.0
.....

Features
~~~~~~~~

* ``Create 'create_token' method in FAB auth manager (#59245)``

Bug Fixes
~~~~~~~~~

* ``Permit 'airflow db migrate -r' with an empty database (#59205)``

Misc
~~~~

* ``Add backcompat for exceptions in providers (#58727)``
* ``Remove global statement from Fab provider (#59018)``
* ``Bump the fab-ui-package-updates group across 1 directory with 2 updates (#58954)``
* ``Bump minimum prek version to 0.2.0 (#58952)``
* ``Use 'HTTP_422_UNPROCESSABLE_CONTENT' instead of 'HTTP_422_UNPROCESSABLE_ENTITY' (#58828)``

Doc-only
~~~~~~~~

* ``docs: adjust misleading docs (#59228)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):


3.0.3
.....

Misc
~~~~

* ``Bump stylelint from 16.25.0 to 16.26.0 in /providers/fab/src/airflow/providers/fab/www in the fab-ui-package-updates group across 1 directory (#58678)``
* ``Remove deprecation warning in Fab provider (#58686)``
* ``Fix mypy errors in providers (#58644)``
* ``Bump webpack from 5.102.1 to 5.103.0 in /providers/fab/src/airflow/providers/fab/www in the fab-ui-package-updates group across 1 directory (#58634)``
* ``Upgrade js-yaml to 4.1.1 in FAB (#58501)``
* ``Migrate FAB PATCH /roles/{name} to FastAPI (#58023)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``

3.0.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix duplicated SQLAlchemy sessions caused transactions fail to close (#58196)``
* ``Fix logout in Fab and Keycloak auth managers (#57992)``
* ``Fix double redirection while authenticating in Fab auth manager (#57993)``
* ``Fix remote user authentication in Fab auth manager (#57775)``
* ``Fix Fab auth manager with 'securecookie' as session backend (#57578)``

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``
* ``Migrate FAB GET /roles/{name} to FastAPI (#58009)``
* ``Migrate FAB DELETE /roles to FastAPI (#57780)``
* ``Bump the fab-ui-package-updates group across 1 directory with 2 updates (#57760)``
* ``Migrate FAB GET /roles to FastAPI (#57411)``
* ``Synchronize boto3 and sqlalchemy dependency lower-binds (#57385)``
* ``Bump the fab-ui-package-updates group across 1 directory with 4 updates (#57160)``
* ``Migrate FAB POST /roles to FastAPI (#57199)``
* ``Upgrade 'flask-appbuilder' to 5.0.1 (#57170)``
* ``refactor: migrate models to use mapped_column for SQLAlchemy 2.0 compatibility (#56827)``

Doc-only
~~~~~~~~

* ``Fixing some typos and spelling errors (#57186)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable pt006 rule and fix new generate errors (#58238)``
   * ``Enable ruff PLW2101,PLW2901,PLW3301 rule (#57700)``
   * ``Revert "Fix duplicated SQLAlchemy sessions caused transactions fail to close (#57815)" (#58097)``
   * ``Fix duplicated SQLAlchemy sessions caused transactions fail to close (#57815)``
   * ``Synchronize default versions in all split .pre-commit-config.yaml (#57851)``
   * ``Fix mypy static errors in fab provider (#57761)``
   * ``Fix mypy static errors in main (#57755)``
   * ``Fix mypy type errors in providers/standard/ in external_task.py for SQLAlchemy 2 migration (#57369)``
   * ``Enable ruff PLW1641 rule (#57679)``
   * ``Extract prek hooks for FAB provider (#57181)``

3.0.1
.....

Bug Fixes
~~~~~~~~~

* ``Update authentication to handle JWT token in backend (#56633)``
* ``Add Werkzeug version check (#56398)``

Misc
~~~~

* ``SQLA2/FAB: fix some type hints (#56928)``
* ``FAB: reuse ORM type hints from common-compat (#56903)``
* ``Bump eslint from 9.37.0 to 9.38.0 in /providers/fab/src/airflow/providers/fab/www in the fab-ui-package-updates group across 1 directory (#56847)``
* ``Bump the fab-ui-package-updates group across 1 directory with 3 updates (#56436)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

Migrated to Flask-appbuilder 5 which removed the authentication type ``AUTH_OID``.
Using the authentication type ``AUTH_OID`` in fab provider is no longer possible.
Applications using ``AUTH_TYPE = AUTH_OID`` must migrate to ``AUTH_OAUTH``.

* ``Upgrade flask-appbuilder to version 5 (#50960)``

Features
~~~~~~~~

* ``Support nested groups resolution for LDAP authentication in Fab auth manager``

Bug Fixes
~~~~~~~~~

* ``Add 'if_not_exists=True' to FAB migration (#56100)``
* ``Add if_not_exists to index creation in migrations (#56328)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix static check error resulting from not rebased change in FAB5 (#56178)``
   * ``Remove placeholder Release Date in changelog and index files (#56056)``
   * ``Prepare fab and amazon providers to release (September 2025) (#56241)``

2.4.4
.....

Bug Fixes
~~~~~~~~~

* ``Override 'get_authorized_connections', 'get_authorized_pools' and 'get_authorized_variables' in Fab auth manager (#55682)``

Misc
~~~~

* ``Bump eslint from 9.35.0 to 9.36.0 in /providers/fab/src/airflow/providers/fab/www in the fab-ui-package-updates group across 1 directory (#55895)``
* ``Move DagBag to airflow/dag_processing (#55139)``

Doc-only
~~~~~~~~

* ``Add SSO integration guide for Apache Airflow (#55281)``
* ``Remove useless Airflow version compatibility checks (#55852)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.4.3
.....


Bug Fixes
~~~~~~~~~

* ``Fix: Clean up FAB permissions when deleting DAGs (#54528)``

Misc
~~~~

* ``Bump stylelint from 16.23.1 to 16.24.0 in /providers/fab/src/airflow/providers/fab/www in the fab-ui-package-updates group across 1 directory (#55382)``
* ``Remove SDK dependency from SerializedDAG (#55538)``
* ``Set flask_limiter>3,<4,!=3.13 (#55592)``
* ``Bump the fab-ui-package-updates group across 1 directory with 3 updates (#55302)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.4.2
.....


Bug Fixes
~~~~~~~~~

* ``Remove query obj from providers fab (#53953)``
* ``fix: Add cookies options into FAB provider.yaml (#54995)``
* ``To fix Fab auth manager returns get_id of integer type where str is expected  (#54384)``
* ``Potential fix for code scanning alert no. 519: Clear-text logging of sensitive information (#54742)``
* ``Create FAB's user/role tables on migration, not only on initdb (#54227)``

Misc
~~~~

* ``Bump the fab-ui-package-updates group across 1 directory with 8 updates (#54517)``
* ``Move DagBag to SDK and make it return SDK DAG objects (#53918)``
* ``Update 'is_authorized_dag' method in 'FabAuthManager' (#54926)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``
* ``Fix Airflow 2 reference in README/index of providers (#55240)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove airflow.models.DAG (#54383)``
   * ``Move secrets_masker over to airflow_shared distribution (#54449)``
   * ``Switch pre-commit to prek (#54258)``
   * ``make bundle_name not nullable (#47592)``

2.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix 'get_authorized_dag_ids' in 'FabAuthManager' (#54276)``
* ``Allow downgrading to 2.11 from 3.x (#54371)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Revert "Allow downgrading to 2.11 from 3.x (#54231)" (#54367)``
   * ``Allow downgrading to 2.11 from 3.x (#54231)``

2.4.0
.....

Features
~~~~~~~~

* ``Create HITL specific permission for core-API (#54043)``

Bug Fixes
~~~~~~~~~

* ``fix: Add FAB configs for cookie security (#53542)``
* ``Fig 'Config' menu item missing in 'FabAuthManager' (#53944)``

Misc
~~~~

* ``Bump the fab-ui-package-updates group across 1 directory with 3 updates (#53941)``
* ``Add UI for human in the loop operators (#53035)``
* ``Bump the fab-ui-package-updates group across 1 directory with 4 updates (#53503)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Remove 'MENU' from 'ResourceMethod' in auth manager (#52731)``

Misc
~~~~

* ``Fix FAB provider in unreachable code (#53436)``
* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup mypy ignore for fab provider in init_jinja_globals (#53328)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Remove unused batch methods from auth manager (#52883)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Bump the fab-ui-package-updates group across 1 directory with 7 updates (#52807)``

Doc-only
~~~~~~~~

* ``Remove extra slash from endpoint URL (#53755)``
* ``Fix spelling of GitHub brand name (#53735)``
* ``Clarify FAB auth provider versioning and webserver_config.py deprecation for Airflow 3.x (#53606)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Cleanup mypy ignore in fab provider where possible (#53282)``

2.3.0
.....

Features
~~~~~~~~

* ``[AIP-68] Support pluginv2 views (#52582)``

Bug Fixes
~~~~~~~~~

* ``Set prefix to generate correctly the FAB Auth Manager API ref (#52329)``
* ``Fix airflow pin for fab provider (#52351)``
* ``Sanitize Username (#52419)``

Misc
~~~~

* ``Drop support for Python 3.9 (#52072)``
* ``Bump the fab-ui-package-updates group across 1 directory with 4 updates (#52108)``

Doc-only
~~~~~~~~

* ``Enable LDAP users to generate an Airflow token with 'FabAuthManager' (#52295)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``remove pytest db_test marker where unnecessary (#52171)``

2.2.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix: allow users with specific DAG permissions to access DAGs when no specific DAG is requested (#51462)``
* ``Fix fab asset compilation hashing script (#51446)``
* ``Fix default setting for hash algorithm for FAB password hash (#51858)``

Misc
~~~~

* ``Add back security api in FAB auth manager (#51578)``
* ``Bump @babel/eslint-parser from 7.27.1 to 7.27.5 in /providers/fab/src/airflow/providers/fab/www in the fab-ui-package-updates group across 1 directory (#51375)``

Doc-only
~~~~~~~~

* ``Fix docstring in 'FabAuthManager' (#51892)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare release for June 2025 provider wave (#51724)``

2.2.0
.....

Features
~~~~~~~~

* ``Add 'airflow db-manager' CLI for managing external databases (#50657)``
* ``Add support for unknown OIDC providers (#50921)``
* ``Move enable_swagger_ui config to api (#50896)``
* ``Move secret_key config to api section (#50839)``
* ``Move webserver config to fab provider (#50774)``
* ``Move webserver config options to api (#50693)``

Misc
~~~~

* ``Bump the fab-ui-package-updates group across 1 directory with 4 updates (#51312)``
* ``Remove unused entries from 'DagAccessEntity' (#51174)``
* ``Bump moment-timezone from 0.5.48 to 0.6.0 in /providers/fab/src/airflow/providers/fab/www in the fab-ui-package-updates group across 1 directory (#51087)``
* ``Bump webpack from 5.99.8 to 5.99.9 in /providers/fab/src/airflow/providers/fab/www in the fab-ui-package-updates group across 1 directory (#50847)``
* ``Bump the fab-ui-package-updates group across 1 directory with 2 updates (#50783)``
* ``Change v1 to v2 in generated OpenAPI schema files (#50705)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Compile FAB assets (#51113)``
   * ``Stabilize FAB asset compilation (#50829)``
   * ``Fixing fab assets generation (#50664)``

2.1.0
.....

Features
~~~~~~~~

* ``Add back ProxyFix Middleware for flask app builder (#49942)``
* ``Move access_denied_message webserver config to fab (#50208)``
* ``Move webserver expose_hostname config to fab (#50269)``

Misc
~~~~

* ``Bump the fab-ui-package-updates group across 1 directory with 4 updates (#50312)``
* ``Bump the fab-ui-package-updates group across 1 directory with 4 updates (#50035)``
* ``Upgrade 'flask-appbuilder' to 4.6.3 in FAB provider (#50513)``

Doc-only
~~~~~~~~

* ``docs: Update oauth keycloak example with new security manager (#50284)``
* ``docs: conditionally render section‐move links in sections‐and‐options include (#50582)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``
   * ``Fix main - Generate FAB assets (#50546)``
   * ``Fix Fab docs (#50594)``

2.0.2
.....

Bug Fixes
~~~~~~~~~

* ``Add read config permission to viewer role in 'FabAuthManager' (#49581)``
* ``Fix infinite redirect in FAB AuthManager caused by mistakenly setting token cookie as secure (#49724)``

Misc
~~~~

* ``Remove some lingering subdag references (#49663)``
* ``Bump stylelint (#49638)``
* ``Bump the fab-ui-package-updates group across 1 directory with 2 updates (#49511)``
* ``Remove old dynamic attr accessing re dag resource in FAB provider (#49669)``

Doc-only
~~~~~~~~

* ``Adding flask app configuration docs to FAB provider (#49492)``
* ``Add note in FAB migration doc (#49423)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``capitalize the term airflow (#49450)``
   * ``Bump the fab-ui-package-updates group across 1 directory with 2 updates (#49792)``
   * ``Prepare docs for Apr ad hoc release of fab and common.compat providers (#49690)``

2.0.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix fab auth manager login (#49292)``

Misc
~~~~
* ``Bump eslint-config-prettier (#49077)``
* ``remove superfluous else block (#49199)``
* ``Change default page_size from 100 to 50 (#49243)``
* ``Make sure all openapi schemes have distinct names (#49290)``
* ``Remove 'STATE_COLORS' from Airflow Local Setting (#49228)``
* ``AIP-38: remove 'default_ui_timezone' (#49176)``
* ``Remove FAB entry from 'NOTICE' (#49063)``
* ``refactor: remove 'navbar_logo_text_color' (#49161)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use contextlib.suppress(exception) instead of try-except-pass and add SIM105 ruff rule (#49251)``
   * ``Add possibility to have extra project metadata in providers (#49306)``
   * ``Quickly bumpv FAB version to 2.0.1 (#49308)``
   * ``Update FAB changelog (#49069)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  The new version of the Fab provider is only compatible with Airflow 3.
  It is impossible to use ``apache-airflow-providers-fab`` >= 2.0 with Airflow 2.X.
  If you use Airflow 2.X, please use ``apache-airflow-providers-fab`` 1.X.

.. warning::
  All deprecated classes, parameters and features have been removed from the Fab provider package.
  The following breaking changes were introduced:

* Removed ``is_authorized_dataset`` method from ``FabAuthManager``. Use ``is_authorized_asset`` instead
* Removed the authentication type ``AUTH_OID``
* Removed ``get_readable_dags`` method from the security manager override
* Removed ``get_editable_dags`` method from the security manager override
* Removed ``get_accessible_dags`` method from the security manager override
* Removed ``get_accessible_dag_ids`` method from the security manager override
* Removed ``prefixed_dag_id`` method from the security manager override
* Removed ``init_role`` method from the security manager override

* ``Prepare FAB provider to set next version as major version (#43939)``
* ``Remove deprecations from fab provider (#44198)``
* ``Rename 'get_permitted_dag_ids' and 'filter_permitted_dag_ids' to 'get_authorized_dag_ids' and 'filter_authorized_dag_ids' (#47640)``
* ``Set simple auth manager as default (#47691)``

Features
~~~~~~~~

* ``Set up JWT token authentication in Fast APIs (#42634)``
* ``AIP-79 Support Airflow 2.x plugins in fast api. Embed a minimal version of the Flask application in fastapi application (#44464)``
* ``AIP 84 - Add auth for asset alias (#47241)``
* ``AIP-81 | AIP-84 | Include Token Generation Endpoints in FAB (#47043)``
* ``AIP-84 Add Auth for DAG Versioning (#47553)``
* ``AIP-84 Add Auth for backfill (#47482)``

Misc
~~~~

* ``Expose security views in Flask application in FAB provider (#46203)``
* ``Fix and simplify 'get_permitted_dag_ids' in auth manager (#47458)``
* ``Get rid of google-re2 as dependency (#47493)``
* ``Introduce 'filter_authorized_menu_items' to filter menu items based on permissions (#47681)``
* ``Remove links to x/twitter.com (#47801)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prevent __init__.py in providers from being modified (#44713)``
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``Prepare docs for Mar 2nd wave of providers (#48383)``
   * ``Prepare docs for Nov 1st wave of providers Dec 2024 (#45042)``
   * ``Prepare documentation for FAB 2.0.0rc2 release (#48745)``
   * ``Remove dev0 suffix from Airflow version (#48934)``
   * ``Improve documentation building iteration (#48760)``
   * ``Prepare docs for Apr 1st wave of providers (#48828)``
   * ``Fix default base value (#49013)``
   * ``(Re)move old dependencies from the old FAB UI (#48007)``
   * ``AIP-38 Fix safari login loop in dev mode (#47859)``
   * ``AIP-38 Move token handling to axios interceptor (#47562)``
   * ``AIP-72: Handle Custom XCom Backend on Task SDK (#47339)``
   * ``AIP-79 Generate assets for Flask application in FAB provider (#44744) (#45060)``
   * ``AIP-81: Flatten core CLI commands (#48224)``
   * ``AIP-83 amendment: Add logic for generating run_id when logical date is None. (#46616)``
   * ``Add 'get_additional_menu_items' in auth manager interface to extend the menu (#47468)``
   * ``Add 'logout' method in auth manager interface (#47573)``
   * ``Add authentication section in FAB auth manager API documentation (#48455)``
   * ``Add back 'get_url_logout' in auth managers but make it optional (#47729)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Add missing methods in fab provider's AirflowAppBuilder class (#45611)``
   * ``Add option in auth manager interface to define FastAPI api (#45009)``
   * ``Add option in auth managers to specify DB manager (#48196)``
   * ``Add run_after column to DagRun model (#45732)``
   * ``Add some typing and require kwargs for auth manager (#47455)``
   * ``Avoid imports from "providers" (#46801)``
   * ``Bump dompurify in /providers/fab/src/airflow/providers/fab/www (#46798)``
   * ``Bump eslint in /providers/fab/src/airflow/providers/fab/www (#48143)``
   * ``Bump eslint-config-prettier (#48206)``
   * ``Bump serialize-javascript, copy-webpack-plugin and terser-webpack-plugin (#46698)``
   * ``Bump the fab-ui-package-updates group across 1 directory with 21 updates (#48414)``
   * ``Bump various providers in preparation for Airflow 3.0.0b4 (#48013)``
   * ``Call 'init' from auth managers only once (#47869)``
   * ``Clean Leftovers of RemovedInAirflow3Warning (#47264)``
   * ``Clean up simple auth and fab provider package json files (#47516)``
   * ``Cleanup leftovers from api connexion (#47490)``
   * ``Convert exceptions raised in Flask application to fastapi exceptions (#45625)``
   * ``Cookies in non TLS mode (#48453)``
   * ``Disable Flask-SQLAlchemy modification tracking in FAB provider (#46249)``
   * ``Do not use FAB auth manager methods in views (#47747)``
   * ``Do not use core Airflow Flask related resources in FAB provider (#45441)``
   * ``Do not use core Airflow Flask related resources in FAB provider (package 'api_connexion') (#45473)``
   * ``Do not use core Airflow Flask related resources in FAB provider (package 'security') (#45471)``
   * ``FAB login. Fix asset URLs and missing alert (#47586)``
   * ``Fix 'conf.get_boolean("api", "ssl_cert")' (#48465)``
   * ``Fix 'get_menu_items' in FAB auth manager (#47688)``
   * ``Fix 'sync-perm' CLI command (#47626)``
   * ``Fix FAB static asset (#46727)``
   * ``Fix new UI when running outside of breeze (#46991)``
   * ``Fix section for base_url in FAB auth manager (#47173)``
   * ``Handle user deletion while being logged in in FAB auth manager (#48754)``
   * ``Implement 'simple_auth_manager_all_admins' in simple auth manager with new auth flow (#47514)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Make FAB auth manager login process compatible with Airflow 3 UI (#45765)``
   * ``Make parameter 'user' mandatory for all methods in the auth manager interface (#45986)``
   * ``Marking fab and common messaging as not ready (#47581)``
   * ``Move "create db from orm" to be a public method in db manager interface (#48000)``
   * ``Move 'airflow.www.auth' to 'airflow.providers.fab.www.auth' (#47307)``
   * ``Move 'airflow/api_fastapi/auth/managers/utils/fab' to FAB provider (#47571)``
   * ``Move 'fastapi-api' command to 'api-server' (#47076)``
   * ``Move FAB session table creation to FAB provider (#47969)``
   * ``Move Literal alias into TYPE_CHECKING block (#45345)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Move api-server to port 8080 (#47310)``
   * ``Move fab provider to new structure (#46144)``
   * ``Move flask-based tests of providers manager to FAB provider tests (#48113)``
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Moving EmptyOperator to standard provider (#46231)``
   * ``Prepare fab ad-hoc release December 2024 (#45218)``
   * ``Add AWS SageMaker Unified Studio Workflow Operator (#45726)``
   * ``Re-work JWT Validation and Generation to use public/private key and official claims (#46981)``
   * ``Rebuild FAB assets (#48116)``
   * ``Relocate airflow.auth to airflow.api_fastapi.auth (#47492)``
   * ``Remove '/webapp' prefix from new UI (#47041)``
   * ``Remove 'airflow.www' module (#47318)``
   * ``Remove 'api_connexion' (#47171)``
   * ``Remove 'is_in_fab' in FAB auth manager (#47465)``
   * ``Remove auth backends from core Airflow (#47399)``
   * ``Remove extra whitespace in provider readme template (#46975)``
   * ``Remove old UI and webserver (#46942)``
   * ``Remove old provider references and replace "new" with just providers (#46810)``
   * ``Remove references of "airflow.www" in FAB provider (#46914)``
   * ``Remove unused code in Fab provider (#47510)``
   * ``Remove unused methods from auth managers (#47316)``
   * ``Remove unused webserver configs (#48066)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Set JWT token to localStorage from cookies (#47432)``
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Stop reserializing DAGs during db migration (#45362)``
   * ``Update FAB auth manager 'get_url_login' method to handle AF2 and AF3 (#46527)``
   * ``Update FAB provider documentation (#48247)``
   * ``Update create token apis in simple auth manager (#48498)``
   * ``Update docstring for users param in auth managers (#47334)``
   * ``Update fast-api generated code after Pydantic upgrade (#48484)``
   * ``Update simple auth manager documentation to include token API (#48454)``
   * ``Upgrade 'copy-webpack-plugin' to latest version in FAB provider (#48399)``
   * ``Upgrade flit to 3.11.0 (#46938)``
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Upgrade ruff to latest version (#48553)``
   * ``Upgrade to FAB 4.5.3 (#45874)``
   * ``Use SimpleAuthManager for standalone (#48036)``
   * ``Use a single http tag to report the server's location to front end, not two (#47572)``
   * ``Use different default algorithms for different werkzeug versions (#46384)``
   * ``feat(AIP-84): add auth to /ui/backfills (#47657)``
   * ``forward port fab 1.5.2 to main branch (#45377)``
   * ``move standard, alibaba and common.sql provider to the new structure (#45964)``
   * Removed ``oauth_whitelists`` property from the security manager override. Use ``oauth_allow_list`` instead
   * ``AIP-81 Move CLI Commands to directories according to Hybrid, Local and Remote (#44538)``


1.5.3
.....

Bug Fixes
~~~~~~~~~

* ``[providers-fab/v1-5] Use different default algorithms for different werkzeug versions (#46384) (#46392)``

Misc
~~~~

* ``[providers-fab/v1-5] Upgrade to FAB 4.5.3 (#45874) (#45918)``


1.5.2
.....

Misc
~~~~

* ``Correctly import isabs from os.path (#45178)``
* ``Invalidate user session on password reset (#45139)``

1.5.1
.....

Bug Fixes
~~~~~~~~~

* ``fab_auth_manager: allow get_user method to return the user authenticated via Kerberos (#43662)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Expand and improve the kerberos api authentication documentation (#43682)``

1.5.0
.....

Features
~~~~~~~~

* ``feat(providers/fab): Use asset in common provider (#43112)``

Bug Fixes
~~~~~~~~~

* ``fix revoke Dag stale permission on airflow < 2.10 (#42844)``
* ``fix(providers/fab): alias is_authorized_dataset to is_authorized_asset (#43469)``
* ``fix: Change CustomSecurityManager method name (#43034)``

Misc
~~~~

* ``Upgrade Flask-AppBuilder to 4.5.2 (#43309)``
* ``Upgrade Flask-AppBuilder to 4.5.1 (#43251)``
* ``Move user and roles schemas to fab provider (#42869)``
* ``Move the session auth backend to FAB auth manager (#42878)``
* ``Add logging to the migration commands (#43516)``
* ``DOC fix documentation error in 'apache-airflow-providers-fab/access-control.rst' (#43495)``
* ``Rename dataset as asset in UI (#43073)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``
   * ``Start porting DAG definition code to the Task SDK (#43076)``
   * ``Prepare docs for Oct 2nd wave of providers (#43409)``
   * ``Prepare docs for Oct 2nd wave of providers RC2 (#43540)``

1.4.1
.....

Misc
~~~~

* ``Update Rest API tests to no longer rely on FAB auth manager. Move tests specific to FAB permissions to FAB provider (#42523)``
* ``Rename dataset related python variable names to asset (#41348)``
* ``Simplify expression for get_permitted_dag_ids query (#42484)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.4.0
.....

Features
~~~~~~~~

* ``Add FAB migration commands (#41804)``
* ``Separate FAB migration from Core Airflow migration (#41437)``

Misc
~~~~

* ``Deprecated kerberos auth removed (#41693)``
* ``Deprecated configuration removed (#42129)``
* ``Move 'is_active' user property to FAB auth manager (#42042)``
* ``Move 'register_views' to auth manager interface (#41777)``
* ``Revert "Provider fab auth manager deprecated methods removed (#41720)" (#41960)``
* ``Provider fab auth manager deprecated methods removed (#41720)``
* ``Make kerberos an optional and devel dependency for impala and fab (#41616)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add TODOs in providers code for Subdag code removal (#41963)``
   * ``Add fixes by breeze/precommit-lint static checks (#41604) (#41618)``
   * ``Fix pre-commit for auto update of fab migration versions (#42382)``
   * ``Handle 'AUTH_ROLE_PUBLIC' in FAB auth manager (#42280)``

1.3.0
.....

Features
~~~~~~~~

* ``Feature: Allow set Dag Run resource into Dag Level permission (#40703)``

Misc
~~~~

* ``Remove deprecated SubDags (#41390)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.2.2
.....

Bug Fixes
~~~~~~~~~

* ``Bug fix: sync perm command not able to use custom security manager (#41020)``
* ``Bump version checked by FAB provider on logout CSRF protection to 2.10.0 (#40784)``

Misc
~~~~

* ``AIP-44 make database isolation mode work in Breeze (#40894)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.2.1
.....

Bug Fixes
~~~~~~~~~

* ``Add backward compatibility to CSRF protection of '/logout' method (#40479)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

1.2.0
.....

Features
~~~~~~~~

* ``Add CSRF protection to "/logout" (#40145)``

Misc
~~~~

* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``
* ``Upgrade to FAB 4.5.0 (#39851)``
* ``fix: sqa deprecations for airflow providers (#39293)``
* ``Add '[webserver]update_fab_perms' to deprecated configs (#40317)``

1.1.1
.....

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``
* ``Simplify action name retrieval in FAB auth manager (#39358)``
* ``Add 'jmespath' as an explicit dependency (#39350)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

1.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Remove plugins permissions from Viewer role (#39254)``
* ``Update 'is_authorized_custom_view' from auth manager to handle custom actions (#39167)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

1.0.4
.....

Bug Fixes
~~~~~~~~~

* ``Remove button for reset my password when we have reset password (#38957)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Activate RUF019 that checks for unnecessary key check (#38950)``


1.0.3
.....

Bug Fixes
~~~~~~~~~

* ``Rename 'allowed_filter_attrs' to 'allowed_sort_attrs' (#38626)``
* ``Fix azure authentication when no email is set (#38872)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``fix: try002 for provider fab (#38801)``

1.0.2
.....

First stable release for the provider


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade FAB to 4.4.1 (#38319)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``Make the method 'BaseAuthManager.is_authorized_custom_view' abstract (#37915)``
   * ``Avoid use of 'assert' outside of the tests (#37718)``
   * ``Resolve G004: Logging statement uses f-string (#37873)``
   * ``Remove useless methods from security manager (#37889)``
   * ``Use 'next' when redirecting (#37904)``
   * ``Add "MENU" permission in auth manager (#37881)``
   * ``Avoid to use too broad 'noqa' (#37862)``
   * ``Add post endpoint for dataset events (#37570)``
   * ``Add "queuedEvent" endpoint to get/delete DatasetDagRunQueue (#37176)``
   * ``Add swagger path to FAB Auth manager and Internal API (#37525)``
   * ``Revoking audit_log permission from all users except admin (#37501)``
   * ``Enable the 'Is Active?' flag by default in user view (#37507)``
   * ``Add comment about versions updated by release manager (#37488)``
   * ``Until we release 2.9.0, we keep airflow >= 2.9.0.dev0 for FAB provider (#37421)``
   * ``Improve suffix handling for provider-generated dependencies (#38029)``

1.0.0 (YANKED)
..............

Initial version of the provider (beta).
