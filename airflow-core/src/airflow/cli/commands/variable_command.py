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
"""Variable subcommands."""

from __future__ import annotations

import json
import os
from json import JSONDecodeError

from sqlalchemy import select

from airflow.cli.simple_table import AirflowConsole
from airflow.cli.utils import print_export_output
from airflow.models import Variable
from airflow.utils import cli as cli_utils
from airflow.utils.cli import suppress_logs_and_warning
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import create_session, provide_session


@suppress_logs_and_warning
@providers_configuration_loaded
def variables_list(args):
    """Display all the variables."""
    with create_session() as session:
        variables = session.scalars(select(Variable)).all()
    AirflowConsole().print_as(data=variables, output=args.output, mapper=lambda x: {"key": x.key})


@suppress_logs_and_warning
@providers_configuration_loaded
def variables_get(args):
    """Display variable by a given name."""
    try:
        if args.default is None:
            var = Variable.get(args.key, deserialize_json=args.json)
            print(var)
        else:
            var = Variable.get(args.key, deserialize_json=args.json, default_var=args.default)
            print(var)
    except (ValueError, KeyError) as e:
        raise SystemExit(str(e).strip("'\""))


@cli_utils.action_cli
@providers_configuration_loaded
def variables_set(args):
    """Create new variable with a given name, value and description."""
    Variable.set(args.key, args.value, args.description, serialize_json=args.json)
    print(f"Variable {args.key} created")


@cli_utils.action_cli
@providers_configuration_loaded
def variables_delete(args):
    """Delete variable by a given name."""
    Variable.delete(args.key)
    print(f"Variable {args.key} deleted")


@cli_utils.action_cli
@providers_configuration_loaded
@provide_session
def variables_import(args, session):
    """Import variables from a given file."""
    if not os.path.exists(args.file):
        raise SystemExit("Missing variables file.")
    with open(args.file) as varfile:
        try:
            var_json = json.load(varfile)
        except JSONDecodeError:
            raise SystemExit("Invalid variables file.")
    suc_count = fail_count = 0
    skipped = set()
    action_on_existing = args.action_on_existing_key
    existing_keys = set()
    if action_on_existing != "overwrite":
        existing_keys = set(session.scalars(select(Variable.key).where(Variable.key.in_(var_json))))
    if action_on_existing == "fail" and existing_keys:
        raise SystemExit(f"Failed. These keys: {sorted(existing_keys)} already exists.")
    for k, v in var_json.items():
        if action_on_existing == "skip" and k in existing_keys:
            skipped.add(k)
            continue
        try:
            value = v
            description = None
            if isinstance(v, dict) and v.get("value"):  # verify that var configuration has value
                value, description = v["value"], v.get("description")
            Variable.set(k, value, description, serialize_json=not isinstance(value, str))
        except Exception as e:
            print(f"Variable import failed: {e!r}")
            fail_count += 1
        else:
            suc_count += 1
    print(f"{suc_count} of {len(var_json)} variables successfully updated.")
    if fail_count:
        print(f"{fail_count} variable(s) failed to be updated.")
    if skipped:
        print(
            f"The variables with these keys: {list(sorted(skipped))} were skipped because they already exists"
        )


@providers_configuration_loaded
def variables_export(args):
    """Export all the variables to the file."""
    var_dict = {}
    with create_session() as session:
        qry = session.scalars(select(Variable))

        data = json.JSONDecoder()
        for var in qry:
            try:
                val = data.decode(var.val)
            except Exception:
                val = var.val
            if var.description:
                var_dict[var.key] = {
                    "value": val,
                    "description": var.description,
                }
            else:
                var_dict[var.key] = val

    with args.file as varfile:
        json.dump(var_dict, varfile, sort_keys=True, indent=4)
        print_export_output("Variables", var_dict, varfile)
