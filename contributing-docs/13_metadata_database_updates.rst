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

Metadata Database Updates
=========================

When developing features, you may need to persist information to the metadata
database. Airflow has `Alembic <https://github.com/sqlalchemy/alembic>`__ built-in
module to handle all schema changes. Alembic must be installed on your
development machine before continuing with migration. If you had made changes to the ORM,
you will need to generate a new migration file. This file will contain the changes to the
database schema that you have made. To generate a new migration file, run the following:


.. code-block:: bash

    # starting at the root of the project
    $ breeze --backend postgres
    $ cd airflow
    $ alembic revision -m "add new field to db" --autogenerate

       Generating
    ~/airflow/airflow/migrations/versions/a1e23c41f123_add_new_field_to_db.py

Note that migration file names are standardized by pre-commit hook ``update-migration-references``, so that they sort alphabetically and indicate
the Airflow version in which they first appear (the alembic revision ID is removed). As a result you should expect to see a pre-commit failure
on the first attempt.  Just stage the modified file and commit again
(or run the hook manually before committing).

After your new migration file is run through pre-commit it will look like this:

.. code-block::

    1234_A_B_C_add_new_field_to_db.py

This represents that your migration is the 1234th migration and expected for release in Airflow version A.B.C.

--------

You can also learn how to setup your `Node environment <14_node_environment_setup.rst>`__ if you want to develop Airflow UI.
