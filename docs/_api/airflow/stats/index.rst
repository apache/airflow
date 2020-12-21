:mod:`airflow.stats`
====================

.. py:module:: airflow.stats


Module Contents
---------------

.. data:: log
   

   

.. py:class:: TimerProtocol

   Bases: :class:`airflow.typing_compat.Protocol`

   Type protocol for StatsLogger.timer

   
   .. method:: __enter__(self)



   
   .. method:: __exit__(self, exc_type, exc_value, traceback)



   
   .. method:: start(self)

      Start the timer



   
   .. method:: stop(self, send=True)

      Stop, and (by default) submit the timer to statsd




.. py:class:: StatsLogger

   Bases: :class:`airflow.typing_compat.Protocol`

   This class is only used for TypeChecking (for IDEs, mypy, pylint, etc)

   
   .. classmethod:: incr(cls, stat: str, count: int = 1, rate: int = 1)

      Increment stat



   
   .. classmethod:: decr(cls, stat: str, count: int = 1, rate: int = 1)

      Decrement stat



   
   .. classmethod:: gauge(cls, stat: str, value: float, rate: int = 1, delta: bool = False)

      Gauge stat



   
   .. classmethod:: timing(cls, stat: str, dt)

      Stats timing



   
   .. classmethod:: timer(cls, *args, **kwargs)

      Timer metric that can be cancelled




.. py:class:: DummyTimer

   No-op timer

   
   .. method:: __enter__(self)



   
   .. method:: __exit__(self, exc_type, exc_value, traceback)



   
   .. method:: start(self)

      Start the timer



   
   .. method:: stop(self, send=True)

      Stop, and (by default) submit the timer to statsd




.. py:class:: DummyStatsLogger

   If no StatsLogger is configured, DummyStatsLogger is used as a fallback

   
   .. classmethod:: incr(cls, stat, count=1, rate=1)

      Increment stat



   
   .. classmethod:: decr(cls, stat, count=1, rate=1)

      Decrement stat



   
   .. classmethod:: gauge(cls, stat, value, rate=1, delta=False)

      Gauge stat



   
   .. classmethod:: timing(cls, stat, dt)

      Stats timing



   
   .. classmethod:: timer(cls, *args, **kwargs)

      Timer metric that can be cancelled




.. data:: ALLOWED_CHARACTERS
   

   

.. function:: stat_name_default_handler(stat_name, max_length=250) -> str
   A function that validate the statsd stat name, apply changes to the stat name
   if necessary and return the transformed stat name.


.. function:: get_current_handler_stat_name_func() -> Callable[[str], str]
   Get Stat Name Handler from airflow.cfg


.. data:: T
   

   

.. function:: validate_stat(fn: T) -> T
   Check if stat name contains invalid characters.
   Log and not emit stats if name is invalid


.. py:class:: AllowListValidator(allow_list=None)

   Class to filter unwanted stats

   
   .. method:: test(self, stat)

      Test if stat is in the Allow List




.. py:class:: SafeStatsdLogger(statsd_client, allow_list_validator=AllowListValidator())

   Statsd Logger

   
   .. method:: incr(self, stat, count=1, rate=1)

      Increment stat



   
   .. method:: decr(self, stat, count=1, rate=1)

      Decrement stat



   
   .. method:: gauge(self, stat, value, rate=1, delta=False)

      Gauge stat



   
   .. method:: timing(self, stat, dt)

      Stats timing



   
   .. method:: timer(self, stat, *args, **kwargs)

      Timer metric that can be cancelled




.. py:class:: SafeDogStatsdLogger(dogstatsd_client, allow_list_validator=AllowListValidator())

   DogStatsd Logger

   
   .. method:: incr(self, stat, count=1, rate=1, tags=None)

      Increment stat



   
   .. method:: decr(self, stat, count=1, rate=1, tags=None)

      Decrement stat



   
   .. method:: gauge(self, stat, value, rate=1, delta=False, tags=None)

      Gauge stat



   
   .. method:: timing(self, stat, dt, tags=None)

      Stats timing



   
   .. method:: timer(self, stat, *args, tags=None, **kwargs)

      Timer metric that can be cancelled




.. py:class:: _Stats(cls, *args, **kwargs)

   Bases: :class:`type`

   .. attribute:: instance
      :annotation: :Optional[StatsLogger]

      

   
   .. method:: __getattr__(cls, name)



   
   .. classmethod:: get_statsd_logger(cls)

      Returns logger for statsd



   
   .. classmethod:: get_dogstatsd_logger(cls)

      Get DataDog statsd logger



   
   .. classmethod:: get_constant_tags(cls)

      Get constant DataDog tags to add to all stats




.. data:: Stats
   :annotation: :StatsLogger

   

