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
"""
Pools sub-commands
"""
import json
import os

from tabulate import tabulate

from airflow.api.client import get_current_api_client
from airflow.utils import cli as cli_utils


def _tabulate_pools(pools, tablefmt="fancy_grid"):
    return "\n%s" % tabulate(pools, ['Pool', 'Slots', 'Description'],
                             tablefmt=tablefmt)


def pool_list(args):
    """Displays info of all the pools"""
    api_client = get_current_api_client()
    pools = api_client.get_pools()
    print(_tabulate_pools(pools=pools, tablefmt=args.output))


def pool_get(args):
    """Displays pool info by a given name"""
    api_client = get_current_api_client()
    pools = [api_client.get_pool(name=args.pool)]
    print(_tabulate_pools(pools=pools, tablefmt=args.output))


@cli_utils.action_logging
def pool_set(args):
    """Creates new pool with a given name and slots"""
    api_client = get_current_api_client()
    pools = [api_client.create_pool(name=args.pool,
                                    slots=args.slots,
                                    description=args.description)]
    print(_tabulate_pools(pools=pools, tablefmt=args.output))


@cli_utils.action_logging
def pool_delete(args):
    """Deletes pool by a given name"""
    api_client = get_current_api_client()
    pools = [api_client.delete_pool(name=args.pool)]
    print(_tabulate_pools(pools=pools, tablefmt=args.output))


@cli_utils.action_logging
def pool_import(args):
    """Imports pools from the file"""
    api_client = get_current_api_client()
    if os.path.exists(args.file):
        pools = pool_import_helper(args.file)
    else:
        print("Missing pools file.")
        pools = api_client.get_pools()
    print(_tabulate_pools(pools=pools, tablefmt=args.output))


def pool_export(args):
    """Exports all of the pools to the file"""
    pools = pool_export_helper(args.file)
    print(_tabulate_pools(pools=pools, tablefmt=args.output))


def pool_import_helper(filepath):
    """Helps import pools from the json file"""
    api_client = get_current_api_client()

    with open(filepath, 'r') as poolfile:
        data = poolfile.read()
    try:  # pylint: disable=too-many-nested-blocks
        pools_json = json.loads(data)
    except Exception as e:  # pylint: disable=broad-except
        print("Please check the validity of the json file: " + str(e))
    else:
        try:
            pools = []
            counter = 0
            for k, v in pools_json.items():
                if isinstance(v, dict) and len(v) == 2:
                    pools.append(api_client.create_pool(name=k,
                                                        slots=v["slots"],
                                                        description=v["description"]))
                    counter += 1
                else:
                    pass
        except Exception:  # pylint: disable=broad-except
            pass
        finally:
            print("{} of {} pool(s) successfully updated.".format(counter, len(pools_json)))
            return pools  # pylint: disable=lost-exception


def pool_export_helper(filepath):
    """Helps export all of the pools to the json file"""
    api_client = get_current_api_client()
    pool_dict = {}
    pools = api_client.get_pools()
    for pool in pools:
        pool_dict[pool[0]] = {"slots": pool[1], "description": pool[2]}
    with open(filepath, 'w') as poolfile:
        poolfile.write(json.dumps(pool_dict, sort_keys=True, indent=4))
    print("{} pools successfully exported to {}".format(len(pool_dict), filepath))
    return pools
