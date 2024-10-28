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

import datetime


def max_partition(
    table,
    schema="default",
    field=None,
    filter_map=None,
    metastore_conn_id="metastore_default",
):
    """
    Get the max partition for a table.

    :param schema: The hive schema the table lives in
    :param table: The hive table you are interested in, supports the dot
        notation as in "my_database.my_table", if a dot is found,
        the schema param is disregarded
    :param metastore_conn_id: The hive connection you are interested in.
        If your default is set you don't need to use this parameter.
    :param filter_map: partition_key:partition_value map used for partition filtering,
                       e.g. {'key1': 'value1', 'key2': 'value2'}.
                       Only partitions matching all partition_key:partition_value
                       pairs will be considered as candidates of max partition.
    :param field: the field to get the max value from. If there's only
        one partition field, this will be inferred

    >>> max_partition("airflow.static_babynames_partitioned")
    '2015-01-01'
    """
    from airflow.providers.apache.hive.hooks.hive import HiveMetastoreHook

    if "." in table:
        schema, table = table.split(".")
    hive_hook = HiveMetastoreHook(metastore_conn_id=metastore_conn_id)
    return hive_hook.max_partition(
        schema=schema, table_name=table, field=field, filter_map=filter_map
    )


def _closest_date(target_dt, date_list, before_target=None) -> datetime.date | None:
    """
    Find the date in a list closest to the target date.

    An optional parameter can be given to get the closest before or after.

    :param target_dt: The target date
    :param date_list: The list of dates to search
    :param before_target: closest before or after the target
    :returns: The closest date
    """

    def time_before(d):
        return target_dt - d if d <= target_dt else datetime.timedelta.max

    def time_after(d):
        return d - target_dt if d >= target_dt else datetime.timedelta.max

    def any_time(d):
        return target_dt - d if d < target_dt else d - target_dt

    if before_target is None:
        return min(date_list, key=any_time).date()
    if before_target:
        return min(date_list, key=time_before).date()
    else:
        return min(date_list, key=time_after).date()


def closest_ds_partition(
    table, ds, before=True, schema="default", metastore_conn_id="metastore_default"
) -> str | None:
    """
    Find the date in a list closest to the target date.

    An optional parameter can be given to get the closest before or after.

    :param table: A hive table name
    :param ds: A datestamp ``%Y-%m-%d`` e.g. ``yyyy-mm-dd``
    :param before: closest before (True), after (False) or either side of ds
    :param schema: table schema
    :param metastore_conn_id: which metastore connection to use
    :returns: The closest date

    >>> tbl = "airflow.static_babynames_partitioned"
    >>> closest_ds_partition(tbl, "2015-01-02")
    '2015-01-01'
    """
    from airflow.providers.apache.hive.hooks.hive import HiveMetastoreHook

    if "." in table:
        schema, table = table.split(".")
    hive_hook = HiveMetastoreHook(metastore_conn_id=metastore_conn_id)
    partitions = hive_hook.get_partitions(schema=schema, table_name=table)
    if not partitions:
        return None
    part_vals = [next(iter(p.values())) for p in partitions]
    if ds in part_vals:
        return ds
    else:
        parts = [datetime.datetime.strptime(pv, "%Y-%m-%d") for pv in part_vals]
        target_dt = datetime.datetime.strptime(ds, "%Y-%m-%d")
        closest_ds = _closest_date(target_dt, parts, before_target=before)
        if closest_ds is not None:
            return closest_ds.isoformat()
    return None
