:mod:`airflow.macros`
=====================

.. py:module:: airflow.macros

.. autoapi-nested-parse::

   Macros.



Submodules
----------
.. toctree::
   :titlesonly:
   :maxdepth: 1

   hive/index.rst


Package Contents
----------------

.. function:: ds_add(ds, days)
   Add or subtract days from a YYYY-MM-DD

   :param ds: anchor date in ``YYYY-MM-DD`` format to add to
   :type ds: str
   :param days: number of days to add to the ds, you can use negative values
   :type days: int

   >>> ds_add('2015-01-01', 5)
   '2015-01-06'
   >>> ds_add('2015-01-06', -5)
   '2015-01-01'


.. function:: ds_format(ds, input_format, output_format)
   Takes an input string and outputs another string
   as specified in the output format

   :param ds: input string which contains a date
   :type ds: str
   :param input_format: input string format. E.g. %Y-%m-%d
   :type input_format: str
   :param output_format: output string format  E.g. %Y-%m-%d
   :type output_format: str

   >>> ds_format('2015-01-01', "%Y-%m-%d", "%m-%d-%y")
   '01-01-15'
   >>> ds_format('1/5/2015', "%m/%d/%Y",  "%Y-%m-%d")
   '2015-01-05'


.. function:: datetime_diff_for_humans(dt, since=None)
   Return a human-readable/approximate difference between two datetimes, or
   one and now.

   :param dt: The datetime to display the diff for
   :type dt: datetime.datetime
   :param since: When to display the date from. If ``None`` then the diff is
       between ``dt`` and now.
   :type since: None or datetime.datetime
   :rtype: str


