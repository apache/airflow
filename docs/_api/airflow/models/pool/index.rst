:mod:`airflow.models.pool`
==========================

.. py:module:: airflow.models.pool


Module Contents
---------------

.. py:class:: PoolStats

   Bases: :class:`airflow.typing_compat.TypedDict`

   Dictionary containing Pool Stats

   .. attribute:: total
      :annotation: :int

      

   .. attribute:: running
      :annotation: :int

      

   .. attribute:: queued
      :annotation: :int

      

   .. attribute:: open
      :annotation: :int

      


.. py:class:: Pool

   Bases: :class:`airflow.models.base.Base`

   the class to get Pool info.

   .. attribute:: __tablename__
      :annotation: = slot_pool

      

   .. attribute:: id
      

      

   .. attribute:: pool
      

      

   .. attribute:: slots
      

      

   .. attribute:: description
      

      

   .. attribute:: DEFAULT_POOL_NAME
      :annotation: = default_pool

      

   
   .. method:: __repr__(self)



   
   .. staticmethod:: get_pool(pool_name, session: Session = None)

      Get the Pool with specific pool name from the Pools.

      :param pool_name: The pool name of the Pool to get.
      :param session: SQLAlchemy ORM Session
      :return: the pool object



   
   .. staticmethod:: get_default_pool(session: Session = None)

      Get the Pool of the default_pool from the Pools.

      :param session: SQLAlchemy ORM Session
      :return: the pool object



   
   .. staticmethod:: slots_stats(*, lock_rows: bool = False, session: Session = None)

      Get Pool stats (Number of Running, Queued, Open & Total tasks)

      If ``lock_rows`` is True, and the database engine in use supports the ``NOWAIT`` syntax, then a
      non-blocking lock will be attempted -- if the lock is not available then SQLAlchemy will throw an
      OperationalError.

      :param lock_rows: Should we attempt to obtain a row-level lock on all the Pool rows returns
      :param session: SQLAlchemy ORM Session



   
   .. method:: to_json(self)

      Get the Pool in a json structure

      :return: the pool object in json format



   
   .. method:: occupied_slots(self, session: Session)

      Get the number of slots used by running/queued tasks at the moment.

      :param session: SQLAlchemy ORM Session
      :return: the used number of slots



   
   .. method:: running_slots(self, session: Session)

      Get the number of slots used by running tasks at the moment.

      :param session: SQLAlchemy ORM Session
      :return: the used number of slots



   
   .. method:: queued_slots(self, session: Session)

      Get the number of slots used by queued tasks at the moment.

      :param session: SQLAlchemy ORM Session
      :return: the used number of slots



   
   .. method:: open_slots(self, session: Session)

      Get the number of slots open at the moment.

      :param session: SQLAlchemy ORM Session
      :return: the number of slots




