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

2.0.0rc2
........

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  Not all of those changes are breaking, but during the release process we do not spend much time
  on properly classifying it. We will do that in the next release.


  * ``Update create token apis in simple auth manager (#48498)``
  * ``Add authentication section in FAB auth manager API documentation (#48455)``
  * ``Update simple auth manager documentation to include token API (#48454)``
  * ``Update fast-api generated code after Pydantic upgrade (#48484)``
  * ``Fix 'conf.get_boolean("api", "ssl_cert")' (#48465)``
  * ``Cookies in non TLS mode (#48453)``
  * ``Upgrade ruff to latest version (#48553)``
  * ``Upgrade 'copy-webpack-plugin' to latest version in FAB provider (#48399)``
  * ``Bump the fab-ui-package-updates group across 1 directory with 21 updates (#48414)``
  * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
  * ``AIP-81: Flatten core CLI commands (#48224)``
  * ``Add option in auth managers to specify DB manager (#48196)``
  * ``Rebuild FAB assets (#48116)``
  * ``Remove unused webserver configs (#48066)``
  * ``Move "create db from orm" to be a public method in db manager interface (#48000)``
  * ``Call 'init' from auth managers only once (#47869)``
  * ``Set simple auth manager as default (#47691)``
  * ``Move FAB session table creation to FAB provider (#47969)``
  * ``AIP-38 Fix safari login loop in dev mode (#47859)``
  * ``AIP-72: Handle Custom XCom Backend on Task SDK (#47339)``
  * ``Remove links to x/twitter.com (#47801)``
  * ``Rename 'get_permitted_dag_ids' and 'filter_permitted_dag_ids' to 'get_authorized_dag_ids' and 'filter_authorized_dag_ids' (#47640)``
  * ``Set JWT token to localStorage from cookies (#47432)``
  * ``Do not use FAB auth manager methods in views (#47747)``
  * ``Re-work JWT Validation and Generation to use public/private key and official claims (#46981)``
  * ``AIP-84 Add Auth for DAG Versioning (#47553)``
  * ``feat(AIP-84): add auth to /ui/backfills (#47657)``
  * ``Introduce 'filter_authorized_menu_items' to filter menu items based on permissions (#47681)``
  * ``Add back 'get_url_logout' in auth managers but make it optional (#47729)``
  * ``Fix 'get_menu_items' in FAB auth manager (#47688)``
  * ``AIP-84 Add Auth for backfill (#47482)``
  * ``Fix 'sync-perm' CLI command (#47626)``
  * ``Add 'get_additional_menu_items' in auth manager interface to extend the menu (#47468)``
  * ``Use a single http tag to report the server's location to front end, not two (#47572)``
  * ``FAB login. Fix asset URLs and missing alert (#47586)``
  * ``Add 'logout' method in auth manager interface (#47573)``
  * ``Move 'airflow/api_fastapi/auth/managers/utils/fab' to FAB provider (#47571)``
  * ``Implement 'simple_auth_manager_all_admins' in simple auth manager with new auth flow (#47514)``
  * ``AIP-38 Move token handling to axios interceptor (#47562)``
  * ``AIP 84 - Add auth for asset alias (#47241)``
  * ``Relocate airflow.auth to airflow.api_fastapi.auth (#47492)``
  * ``Clean up simple auth and fab provider package json files (#47516)``
  * ``Remove unused code in Fab provider (#47510)``
  * ``Cleanup leftovers from api connexion (#47490)``
  * ``Get rid of google-re2 as dependency (#47493)``
  * ``Remove 'is_in_fab' in FAB auth manager (#47465)``
  * ``Fix and simplify 'get_permitted_dag_ids' in auth manager (#47458)``
  * ``Add some typing and require kwargs for auth manager (#47455)``
  * ``Clean Leftovers of RemovedInAirflow3Warning (#47264)``
  * ``Add AWS SageMaker Unified Studio Workflow Operator (#45726)``
  * ``Remove unused methods from auth managers (#47316)``
  * ``Update docstring for users param in auth managers (#47334)``
  * ``AIP-81 | AIP-84 | Include Token Generation Endpoints in FAB (#47043)``
  * ``Move 'airflow.www.auth' to 'airflow.providers.fab.www.auth' (#47307)``
  * ``Remove 'api_connexion' (#47171)``
  * ``Remove '/webapp' prefix from new UI (#47041)``
  * ``Upgrade flit to 3.11.0 (#46938)``
  * ``Remove references of "airflow.www" in FAB provider (#46914)``
  * ``Bump dompurify in /providers/fab/src/airflow/providers/fab/www (#46798)``
  * ``Remove old provider references and replace "new" with just providers (#46810)``
  * ``Avoid imports from "providers" (#46801)``
  * ``Move 'fastapi-api' command to 'api-server' (#47076)``
  * ``Fix FAB static asset (#46727)``
  * ``AIP-79 Generate assets for Flask application in FAB provider (#44744) (#45060)``
  * ``Bump serialize-javascript, copy-webpack-plugin and terser-webpack-plugin (#46698)``
  * ``AIP-83 amendment: Add logic for generating run_id when logical date is None. (#46616)``
  * ``Update FAB auth manager 'get_url_login' method to handle AF2 and AF3 (#46527)``
  * ``Disable Flask-SQLAlchemy modification tracking in FAB provider (#46249)``
  * ``Expose security views in Flask application in FAB provider (#46203)``
  * ``Make parameter 'user' mandatory for all methods in the auth manager interface (#45986)``
  * ``Upgrade to FAB 4.5.3 (#45874)``
  * ``Make FAB auth manager login process compatible with Airflow 3 UI (#45765)``
  * ``Convert exceptions raised in Flask application to fastapi exceptions (#45625)``
  * ``Add missing methods in fab provider's AirflowAppBuilder class (#45611)``
  * ``Do not use core Airflow Flask related resources in FAB provider (package 'security') (#45471)``
  * ``Do not use core Airflow Flask related resources in FAB provider (package 'api_connexion') (#45473)``
  * ``Do not use core Airflow Flask related resources in FAB provider (#45441)``
  * ``forward port fab 1.5.2 to main branch (#45377)``
  * ``Prepare fab ad-hoc release December 2024 (#45218)``
  * ``Add option in auth manager interface to define FastAPI api (#45009)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Prepare docs for Mar 2nd wave of providers (#48383)``
   * ``Bump eslint-config-prettier (#48206)``
   * ``(Re)move old dependencies from the old FAB UI (#48007)``
   * ``Bump eslint in /providers/fab/src/airflow/providers/fab/www (#48143)``
   * ``Move flask-based tests of providers manager to FAB provider tests (#48113)``
   * ``Use SimpleAuthManager for standalone (#48036)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Bump various providers in preparation for Airflow 3.0.0b4 (#48013)``
   * ``Marking fab and common messaging as not ready (#47581)``
   * ``Remove auth backends from core Airflow (#47399)``
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Remove 'airflow.www' module (#47318)``
   * ``Move api-server to port 8080 (#47310)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Fix section for base_url in FAB auth manager (#47173)``
   * ``Remove old UI and webserver (#46942)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``
   * ``Fix new UI when running outside of breeze (#46991)``
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Moving EmptyOperator to standard provider (#46231)``
   * ``Use different default algorithms for different werkzeug versions (#46384)``
   * ``Add run_after column to DagRun model (#45732)``
   * ``Move fab provider to new structure (#46144)``
   * ``move standard, alibaba and common.sql provider to the new structure (#45964)``
   * ``Move Literal alias into TYPE_CHECKING block (#45345)``
   * ``Stop reserializing DAGs during db migration (#45362)``
   * ``Prepare docs for Nov 1st wave of providers Dec 2024 (#45042)``

2.0.0b1
.......

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
* Removed ``oauth_whitelists`` property from the security manager override. Use ``oauth_allow_list`` instead
* Removed the authentication type ``AUTH_OID``
* Removed ``get_readable_dags`` method from the security manager override
* Removed ``get_editable_dags`` method from the security manager override
* Removed ``get_accessible_dags`` method from the security manager override
* Removed ``get_accessible_dag_ids`` method from the security manager override
* Removed ``prefixed_dag_id`` method from the security manager override
* Removed ``init_role`` method from the security manager override

* ``Prepare FAB provider to set next version as major version (#43939)``
* ``Remove deprecations from fab provider (#44198)``

Features
~~~~~~~~

* ``Set up JWT token authentication in Fast APIs (#42634)``
* ``AIP-79 Support Airflow 2.x plugins in fast api. Embed a minimal version of the Flask application in fastapi application (#44464)``


Misc
~~~~

* ``AIP-81 Move CLI Commands to directories according to Hybrid, Local and Remote (#44538)``

.. Review and move the new changes to one of the sections above:
   * ``Prevent __init__.py in providers from being modified (#44713)``
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``

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

.. Review and move the new changes to one of the sections above:
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
