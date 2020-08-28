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
import sys

from sqlalchemy.exc import SQLAlchemyError

from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.secrets.local_filesystem import load_variables
from airflow.utils import cli as cli_utils
from airflow.utils.session import create_session


def variables_list(args):
    """Displays all of the variables"""
    with create_session() as session:
        variables = session.query(Variable)
    print("\n".join(var.key for var in variables))


def variables_get(args):
    """Displays variable by a given name"""
    try:
        if args.default is None:
            var = Variable.get(
                args.key,
                deserialize_json=args.json
            )
            print(var)
        else:
            var = Variable.get(
                args.key,
                deserialize_json=args.json,
                default_var=args.default
            )
            print(var)
    except (ValueError, KeyError) as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)


@cli_utils.action_logging
def variables_set(args):
    """Creates new variable with a given name and value"""
    Variable.set(args.key, args.value, serialize_json=args.json)


@cli_utils.action_logging
def variables_delete(args):
    """Deletes variable by a given name"""
    Variable.delete(args.key)


DIS_RESTRICT = 'restrict'
DIS_OVERWRITE = 'overwrite'
DIS_IGNORE = 'ignore'
CREATED = 'created'
DISPOSITIONS = [DIS_RESTRICT, DIS_OVERWRITE, DIS_IGNORE]


def _prep_import_status_msgs(var_status_map):
    """Prepare import variable status messages"""
    msg = "\n"
    for status, vars_list in var_status_map.items():
        if len(vars_list) == 0:
            continue

        msg = msg + status + " : \n\t"
        for key in vars_list:
            msg = msg + '\n\t{key}\n'.format(key=key)
    return msg


@cli_utils.action_logging
def variables_import(args):
    """Imports variables from a given file"""

    try:
        vars_map = load_variables(args.file)
    except AirflowException as e:
        print(e)
        return

    if not args.conflict_disposition:
        disposition = DIS_RESTRICT
    else:
        disposition = args.conflict_disposition

    var_status_map = {
        DIS_OVERWRITE: [],
        DIS_IGNORE: [],
        CREATED: []
    }
    try:
        with create_session() as session:
            for key, val in vars_map.items():
                vars_row = Variable.get_variable_from_secrets(key)
                if not vars_row:
                    session.add(Variable(key=key, val=val))
                    session.flush()
                    var_status_map[CREATED].append(key)
                elif disposition == DIS_OVERWRITE:
                    Variable.set(key=key, value=val, session=session)
                    session.flush()
                    var_status_map[DIS_OVERWRITE].append(key)
                elif disposition == DIS_IGNORE:
                    var_status_map[DIS_IGNORE].append(key)
                else:
                    msg = "\nVariable with `key`={key} already exists"
                    msg = msg.format(key=key)
                    raise AirflowException(msg)

            print(_prep_import_status_msgs(var_status_map))

    except (SQLAlchemyError, AirflowException) as e:
        print(e)
        session.rollback()

    finally:
        session.close()


def variables_export(args):
    """Exports all of the variables to the file"""
    _variable_export_helper(args.file)


def _variable_export_helper(filepath):
    """Helps export all of the variables to the file"""
    var_dict = {}
    with create_session() as session:
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
