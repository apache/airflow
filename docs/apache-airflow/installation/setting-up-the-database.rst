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

Setting up the database
-----------------------

Airflow requires a database. If you're just experimenting and learning Airflow, you can stick with the
default SQLite option. If you don't want to use SQLite, then take a look at
:doc:`/howto/set-up-database` to setup a different database.

Usually, you need to run ``airflow db upgrade`` in order to create the database schema that Airflow can use.

Similarly, upgrading Airflow usually requires an extra step of upgrading the database. This is done
with ``airflow db upgrade`` CLI command. You should make sure that Airflow components are
not running while the upgrade is being executed.

In some deployments, such as :doc:`helm-chart:index`, both initializing and running the database migration
is executed automatically when Airflow is upgraded.

Sometimes, after the upgrade, you are also supposed to do some post-migration actions.
See :doc:`/installation/upgrading` for more details about upgrading and doing post-migration actions.
