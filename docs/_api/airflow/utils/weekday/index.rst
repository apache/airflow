:mod:`airflow.utils.weekday`
============================

.. py:module:: airflow.utils.weekday

.. autoapi-nested-parse::

   Get the ISO standard day number of the week from a given day string



Module Contents
---------------

.. py:class:: WeekDay

   Bases: :class:`enum.IntEnum`

   Python Enum containing Days of the Week

   .. attribute:: MONDAY
      :annotation: = 1

      

   .. attribute:: TUESDAY
      :annotation: = 2

      

   .. attribute:: WEDNESDAY
      :annotation: = 3

      

   .. attribute:: THURSDAY
      :annotation: = 4

      

   .. attribute:: FRIDAY
      :annotation: = 5

      

   .. attribute:: SATURDAY
      :annotation: = 6

      

   .. attribute:: SUNDAY
      :annotation: = 7

      

   
   .. classmethod:: get_weekday_number(cls, week_day_str)

      Return the ISO Week Day Number for a Week Day

      :param week_day_str: Full Name of the Week Day. Example: "Sunday"
      :type week_day_str: str
      :return: ISO Week Day Number corresponding to the provided Weekday




