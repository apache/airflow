# -*- coding: utf-8 -*-
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
"""Variable subcommands"""
import json
import os

from airflow.models import Variable
from airflow.utils import cli as cli_utils, db


def variables_list(args):
    """Displays all of the variables"""
    with db.create_session() as session:
        variables = session.query(Variable)
    print("\n".join(var.key for var in variables))


def variables_get(args):
    """Displays variable by a given name"""
    try:
        var = Variable.get(args.key,
                           deserialize_json=args.json,
                           default_var=args.default)
        print(var)
    except ValueError as e:
        print(e)


@cli_utils.action_logging
def variables_set(args):
    """Creates new variable with a given name and value"""
    Variable.set(args.key, args.value, serialize_json=args.json)


@cli_utils.action_logging
def variables_delete(args):
    """Deletes variable by a given name"""
    Variable.delete(args.key)


@cli_utils.action_logging
def variables_import(args):
    """Imports variables from a given file"""
    if os.path.exists(args.file):
        _import_helper(args.file)
    else:
        print("Missing variables file.")


def variables_export(args):
    """Exports all of the variables to the file"""
    _variable_export_helper(args.file)


def _import_helper(filepath):
    """Helps import variables from the file"""
    with open(filepath, 'r') as varfile:
        data = varfile.read()

    try:
        var_json = json.loads(data)
    except Exception:  # pylint: disable=broad-except
        print("Invalid variables file.")
    else:
        suc_count = fail_count = 0
        for k, v in var_json.items():
            try:
                Variable.set(k, v, serialize_json=not isinstance(v, str))
            except Exception as e:  # pylint: disable=broad-except
                print('Variable import failed: {}'.format(repr(e)))
                fail_count += 1
            else:
                suc_count += 1
        print("{} of {} variables successfully updated.".format(suc_count, len(var_json)))
        if fail_count:
            print("{} variable(s) failed to be updated.".format(fail_count))


def _variable_export_helper(filepath):
    """Helps export all of the variables to the file"""
    var_dict = {}
    with db.create_session() as session:
        qry = session.query(Variable).all()

        data = json.JSONDecoder()
        for var in qry:
            try:
                val = data.decode(var.val)
            except Exception:  # pylint: disable=broad-except
                val = var.val
            var_dict[var.key] = val

    with open(filepath, 'w') as varfile:
        varfile.write(json.dumps(var_dict, sort_keys=True, indent=4))
    print("{} variables successfully exported to {}".format(len(var_dict), filepath))
