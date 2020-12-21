:mod:`airflow.utils.timezone`
=============================

.. py:module:: airflow.utils.timezone


Module Contents
---------------

.. data:: utc
   

   

.. function:: is_localized(value)
   Determine if a given datetime.datetime is aware.
   The concept is defined in Python's docs:
   http://docs.python.org/library/datetime.html#datetime.tzinfo
   Assuming value.tzinfo is either None or a proper datetime.tzinfo,
   value.utcoffset() implements the appropriate logic.


.. function:: is_naive(value)
   Determine if a given datetime.datetime is naive.
   The concept is defined in Python's docs:
   http://docs.python.org/library/datetime.html#datetime.tzinfo
   Assuming value.tzinfo is either None or a proper datetime.tzinfo,
   value.utcoffset() implements the appropriate logic.


.. function:: utcnow() -> dt.datetime
   Get the current date and time in UTC

   :return:


.. function:: utc_epoch() -> dt.datetime
   Gets the epoch in the users timezone

   :return:


.. function:: convert_to_utc(value)
   Returns the datetime with the default timezone added if timezone
   information was not associated

   :param value: datetime
   :return: datetime with tzinfo


.. function:: make_aware(value, timezone=None)
   Make a naive datetime.datetime in a given time zone aware.

   :param value: datetime
   :param timezone: timezone
   :return: localized datetime in settings.TIMEZONE or timezone


.. function:: make_naive(value, timezone=None)
   Make an aware datetime.datetime naive in a given time zone.

   :param value: datetime
   :param timezone: timezone
   :return: naive datetime


.. function:: datetime(*args, **kwargs)
   Wrapper around datetime.datetime that adds settings.TIMEZONE if tzinfo not specified

   :return: datetime.datetime


.. function:: parse(string: str, timezone=None) -> DateTime
   Parse a time string and return an aware datetime

   :param string: time string


