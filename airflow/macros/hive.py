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

import datetime
from typing import Any, Dict, Iterable, Optional


def max_partition(
    table: str,
    schema: str = "default",
    field: Optional[str] = None,
    filter_map: Optional[Dict[str, Any]] = None,
    metastore_conn_id: str = "metastore_default",
) -> Any:
    """Get the max partition for a table.

    Example::

        >>> max_partition('airflow.static_babynames_partitioned')
        '2015-01-01'

    :param table: The Hive table you are interested in. This supports the dot
        notation as in ``my_database.my_table``. If a dot is found, the *schema*
        parameter is ignored.
    :type table: str
    :param schema: The Hive schema the table lives in. This is ignored if
        *table* uses the dot notation. The default is ``default``.
    :type schema: str
    :param field: The field to get the max value from. This can be omitted if
        there's only one partition field.
    :type field: str
    :param filter_map: If given, a *partition_key: partition_value* mapping used
        for partition filtering, e.g. ``{'key1': 'value1', 'key2': 'value2'}``.
        Only partitions matching all entries in the mapping will be considered
        as candidates of the max partition.
    :type filter_map: dict[str, Any]
    :param metastore_conn_id: The Hive connection ID. The default is the same as
        ``HiveMetastoreHook.default_conn_name`` (``metastore_default``).
    :type metastore_conn_id: str
    """
    from airflow.providers.apache.hive.hooks.hive import HiveMetastoreHook

    if '.' in table:
        schema, table = table.split('.')
    hive_hook = HiveMetastoreHook(metastore_conn_id=metastore_conn_id)
    return hive_hook.max_partition(schema=schema, table_name=table, field=field, filter_map=filter_map)


def _closest_date(
    target_dt: datetime.date,
    date_list: Iterable[datetime.date],
    before_target: Optional[bool] = None,
) -> datetime.date:
    """Find the date in a list closest to the target date.

    An optional parameter can be given to get the closest before or after.

    :param target_dt: The target date.
    :type target_dt: datetime.date
    :param date_list: List of dates to search.
    :type date_list: list[datetime.date]
    :param before_target: Consider only dates before or after *target_dt*. If
        *None* (default), all dates are considered. If *True*, only dates before
        *target_dt* are considered. If *False*, only dates after *target_dt* are
        considered.
    :type before_target: bool | None
    :returns: The closest date.
    :rtype: datetime.date
    """
    if before_target is None:

        def key(d: datetime.date) -> datetime.timedelta:
            return abs(target_dt - d)

    elif before_target:

        def key(d: datetime.date) -> datetime.timedelta:
            if d <= target_dt:
                return target_dt - d
            return datetime.timedelta.max

    else:

        def key(d: datetime.date) -> datetime.timedelta:
            if d >= target_dt:
                return d - target_dt
            return datetime.timedelta.max

    closest_date = min(date_list, key=key)
    if isinstance(closest_date, datetime.datetime):
        return closest_date.date()
    return closest_date


def closest_ds_partition(
    table: str,
    ds: str,
    before: bool = True,
    schema: str = "default",
    metastore_conn_id: str = "metastore_default",
) -> Optional[str]:
    """Find the date in a list closest to the target date.

    An optional parameter can be given to get the closest before or after.

    Example::

        >>> tbl = 'airflow.static_babynames_partitioned'
        >>> closest_ds_partition(tbl, '2015-01-02')
        '2015-01-01'

    :param table: The Hive table name. This supports the dot notation as in
        ``my_database.my_table``. If a dot is found, the *schema* parameter is
        ignored.
    :type table: str
    :param ds: The target date, in ``%Y-%m-%d`` format.
    :type ds: str
    :param before: Consider only dates before or after *ds*. If *None*
        (default), all dates are considered. If *True*, only dates before *ds*
        are considered. If *False*, only dates after *ds* are considered.
    :type before: bool | None
    :param schema: The Hive schema the table lives in. This is ignored if
        *table* uses the dot notation. The default is ``default``.
    :type schema: str
    :param metastore_conn_id: The Hive connection ID. The default is the same as
        ``HiveMetastoreHook.default_conn_name`` (``metastore_default``).
    :type metastore_conn_id: str
    :returns: The date closest to *ds*, or *None* if no matching date is found.
    :rtype: str | None
    """
    from airflow.providers.apache.hive.hooks.hive import HiveMetastoreHook

    if '.' in table:
        schema, table = table.split('.')
    hive_hook = HiveMetastoreHook(metastore_conn_id=metastore_conn_id)
    partitions = hive_hook.get_partitions(schema=schema, table_name=table)
    if not partitions:
        return None
    part_vals = [list(p.values())[0] for p in partitions]
    if ds in part_vals:
        return ds
    parts = [datetime.datetime.strptime(pv, '%Y-%m-%d') for pv in part_vals]
    if not parts:
        return None
    target_dt = datetime.datetime.strptime(ds, '%Y-%m-%d')
    closest_ds = _closest_date(target_dt, parts, before_target=before)
    return closest_ds.isoformat()
