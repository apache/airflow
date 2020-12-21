:mod:`airflow.utils.dates`
==========================

.. py:module:: airflow.utils.dates


Module Contents
---------------

.. data:: cron_presets
   :annotation: :Dict[str, str]

   

.. function:: date_range(start_date: datetime, end_date: Optional[datetime] = None, num: Optional[int] = None, delta: Optional[Union[str, timedelta, relativedelta]] = None) -> List[datetime]
   Get a set of dates as a list based on a start, end and delta, delta
   can be something that can be added to `datetime.datetime`
   or a cron expression as a `str`

   .. code-block:: python

       date_range(datetime(2016, 1, 1), datetime(2016, 1, 3), delta=timedelta(1))
           [datetime.datetime(2016, 1, 1, 0, 0), datetime.datetime(2016, 1, 2, 0, 0),
           datetime.datetime(2016, 1, 3, 0, 0)]
       date_range(datetime(2016, 1, 1), datetime(2016, 1, 3), delta='0 0 * * *')
           [datetime.datetime(2016, 1, 1, 0, 0), datetime.datetime(2016, 1, 2, 0, 0),
           datetime.datetime(2016, 1, 3, 0, 0)]
       date_range(datetime(2016, 1, 1), datetime(2016, 3, 3), delta="0 0 0 * *")
           [datetime.datetime(2016, 1, 1, 0, 0), datetime.datetime(2016, 2, 1, 0, 0),
           datetime.datetime(2016, 3, 1, 0, 0)]

   :param start_date: anchor date to start the series from
   :type start_date: datetime.datetime
   :param end_date: right boundary for the date range
   :type end_date: datetime.datetime
   :param num: alternatively to end_date, you can specify the number of
       number of entries you want in the range. This number can be negative,
       output will always be sorted regardless
   :type num: int
   :param delta: step length. It can be datetime.timedelta or cron expression as string
   :type delta: datetime.timedelta or str or dateutil.relativedelta


.. function:: round_time(dt, delta, start_date=timezone.make_aware(datetime.min))
   Returns the datetime of the form start_date + i * delta
   which is closest to dt for any non-negative integer i.
   Note that delta may be a datetime.timedelta or a dateutil.relativedelta
   >>> round_time(datetime(2015, 1, 1, 6), timedelta(days=1))
   datetime.datetime(2015, 1, 1, 0, 0)
   >>> round_time(datetime(2015, 1, 2), relativedelta(months=1))
   datetime.datetime(2015, 1, 1, 0, 0)
   >>> round_time(datetime(2015, 9, 16, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
   datetime.datetime(2015, 9, 16, 0, 0)
   >>> round_time(datetime(2015, 9, 15, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
   datetime.datetime(2015, 9, 15, 0, 0)
   >>> round_time(datetime(2015, 9, 14, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
   datetime.datetime(2015, 9, 14, 0, 0)
   >>> round_time(datetime(2015, 9, 13, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
   datetime.datetime(2015, 9, 14, 0, 0)


.. function:: infer_time_unit(time_seconds_arr)
   Determine the most appropriate time unit for an array of time durations
   specified in seconds.
   e.g. 5400 seconds => 'minutes', 36000 seconds => 'hours'


.. function:: scale_time_units(time_seconds_arr, unit)
   Convert an array of time durations in seconds to the specified time unit.


.. function:: days_ago(n, hour=0, minute=0, second=0, microsecond=0)
   Get a datetime object representing `n` days ago. By default the time is
   set to midnight.


.. function:: parse_execution_date(execution_date_str)
   Parse execution date string to datetime object.


