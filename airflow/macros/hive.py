import datetime


def max_partition(
        table, schema="default", field=None, filter=None,
        metastore_conn_id='metastore_default'):
    '''
    Gets the max partition for a table.

    :param schema: The hive schema the table lives in
    :type schema: string
    :param table: The hive table you are interested in, supports the dot
        notation as in "my_database.my_table", if a dot is found,
        the schema param is disregarded
    :type table: string
    :param hive_conn_id: The hive connection you are interested in.
        If your default is set you don't need to use this parameter.
    :type hive_conn_id: string
    :param filter: filter on a subset of partition as in
        `sub_part='specific_value'`
    :type filter: string
    :param field: the field to get the max value from. If there's only
        one partition field, this will be inferred

    >>> max_partition('airflow.static_babynames_partitioned')
    '2015-01-01'
    '''
    from airflow.hooks import HiveMetastoreHook
    if '.' in table:
        schema, table = table.split('.')
    hh = HiveMetastoreHook(metastore_conn_id=metastore_conn_id)
    return hh.max_partition(
        schema=schema, table_name=table, field=field, filter=filter)


def _closest_date(target_dt, date_list, before_target=None):
    '''
    This function finds the date in a list closest to the target date.
    An optional parameter can be given to get the closest before or after.

    :param target_dt: The target date
    :type target_dt: datetime.date
    :param date_list: The list of dates to search
    :type date_list: datetime.date list
    :param before_target: closest before or after the target
    :type before_target: bool or None
    :returns: The closest date
    :rtype: datetime.date or None
    '''
    fb = lambda d: d - target_dt if d >= target_dt else datetime.timedelta.max
    fa = lambda d: d - target_dt if d <= target_dt else datetime.timedelta.min
    fnone = lambda d: target_dt - d if d < target_dt else d - target_dt
    if before_target is None:
        return min(date_list, key=fnone).date()
    if before_target:
        return min(date_list, key=fb).date()
    else:
        return min(date_list, key=fa).date()


def closest_ds_partition(
        table, ds, before=True, schema="default",
        metastore_conn_id='metastore_default'):
    '''
    This function finds the date in a list closest to the target date.
    An optional parameter can be given to get the closest before or after.

    :param table: A hive table name
    :type table: str
    :param ds: A datestamp ``%Y-%m-%d`` e.g. ``yyyy-mm-dd``
    :type ds: datetime.date list
    :param before: closest before (True), after (False) or either side of ds
    :type before: bool or None
    :returns: The closest date
    :rtype: str or None

    >>> tbl = 'airflow.static_babynames_partitioned'
    >>> closest_ds_partition(tbl, '2015-01-02')
    '2015-01-01'
    '''
    from airflow.hooks import HiveMetastoreHook
    if '.' in table:
        schema, table = table.split('.')
    hh = HiveMetastoreHook(metastore_conn_id=metastore_conn_id)
    partitions = hh.get_partitions(schema=schema, table_name=table)
    if not partitions:
        return None
    part_vals = [p.values()[0] for p in partitions]
    if ds in part_vals:
        return ds
    else:
        parts = [datetime.datetime.strptime(pv, '%Y-%m-%d')
                 for pv in part_vals]
        target_dt = datetime.datetime.strptime(ds, '%Y-%m-%d')
        closest_ds = _closest_date(target_dt, parts, before_target=before)
        return closest_ds.isoformat()
