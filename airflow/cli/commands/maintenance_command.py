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
"""Maintenance sub-commands"""
from airflow.utils.metastore_cleanup import objects_dict, run_cleanup

all_tables = list(sorted(objects_dict.keys()))


def cleanup_tables(args):
    """Purges old records in metastore database"""
    run_cleanup(
        table_names=args.tables,
        dry_run=args.dry_run,
        clean_before_timestamp=args.clean_before_timestamp,
        verbose=args.verbose,
        confirm=not args.yes,
    )
