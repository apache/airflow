#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import contextlib
import sys
from logging import getLogger
from logging.config import fileConfig

from alembic import context

from airflow import models, settings
from airflow.utils.db import compare_server_default, compare_type


def include_object(_, name, type_, *args):
    """Filter objects for autogenerating revisions."""
    # Ignore the sqlite_sequence table, which is an internal SQLite construct
    if name == "sqlite_sequence":
        return False
    # Only create migrations for objects that are in the target metadata
    if type_ == "table" and name not in target_metadata.tables:
        return False
    return True


# Make sure everything is imported so that alembic can find it all
models.import_all_models()

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if not getLogger().handlers:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = models.base.Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

# version table
version_table = "alembic_version"


def run_migrations_offline():
    """
    Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        url=settings.SQL_ALCHEMY_CONN,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=compare_type,
        compare_server_default=compare_server_default,
        render_as_batch=True,
        include_object=include_object,
        version_table=version_table,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """
    Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    def process_revision_directives(context, revision, directives):
        if getattr(config.cmd_opts, "autogenerate", False):
            script = directives[0]
            if script.upgrade_ops and script.upgrade_ops.is_empty():
                directives[:] = []
                print("No change detected in ORM schema, skipping revision.")

    with contextlib.ExitStack() as stack:
        connection = config.attributes.get("connection", None)

        if not connection:
            connection = stack.push(settings.engine.connect())

        context.configure(
            connection=connection,
            transaction_per_migration=True,
            target_metadata=target_metadata,
            compare_type=compare_type,
            compare_server_default=compare_server_default,
            include_object=include_object,
            render_as_batch=True,
            process_revision_directives=process_revision_directives,
            version_table=version_table,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

if "airflow.www.app" in sys.modules:
    # Already imported, make sure we clear out any cached app
    from airflow.www.app import purge_cached_app

    purge_cached_app()
