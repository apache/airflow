:mod:`airflow.utils.sqlalchemy`
===============================

.. py:module:: airflow.utils.sqlalchemy


Module Contents
---------------

.. data:: log
   

   

.. data:: utc
   

   

.. data:: using_mysql
   

   

.. py:class:: UtcDateTime

   Bases: :class:`sqlalchemy.types.TypeDecorator`

   Almost equivalent to :class:`~sqlalchemy.types.DateTime` with
   ``timezone=True`` option, but it differs from that by:

   - Never silently take naive :class:`~datetime.datetime`, instead it
     always raise :exc:`ValueError` unless time zone aware value.
   - :class:`~datetime.datetime` value's :attr:`~datetime.datetime.tzinfo`
     is always converted to UTC.
   - Unlike SQLAlchemy's built-in :class:`~sqlalchemy.types.DateTime`,
     it never return naive :class:`~datetime.datetime`, but time zone
     aware value, even with SQLite or MySQL.
   - Always returns DateTime in UTC

   .. attribute:: impl
      

      

   
   .. method:: process_bind_param(self, value, dialect)



   
   .. method:: process_result_value(self, value, dialect)

      Processes DateTimes from the DB making sure it is always
      returning UTC. Not using timezone.convert_to_utc as that
      converts to configured TIMEZONE while the DB might be
      running with some other setting. We assume UTC datetimes
      in the database.




.. py:class:: Interval

   Bases: :class:`sqlalchemy.types.TypeDecorator`

   Base class representing a time interval.

   .. attribute:: impl
      

      

   .. attribute:: attr_keys
      

      

   
   .. method:: process_bind_param(self, value, dialect)



   
   .. method:: process_result_value(self, value, dialect)




.. function:: skip_locked(session: Session) -> Dict[str, Any]
   Return kargs for passing to `with_for_update()` suitable for the current DB engine version.

   We do this as we document the fact that on DB engines that don't support this construct, we do not
   support/recommend running HA scheduler. If a user ignores this and tries anyway everything will still
   work, just slightly slower in some circumstances.

   Specifically don't emit SKIP LOCKED for MySQL < 8, or MariaDB, neither of which support this construct

   See https://jira.mariadb.org/browse/MDEV-13115


.. function:: nowait(session: Session) -> Dict[str, Any]
   Return kwargs for passing to `with_for_update()` suitable for the current DB engine version.

   We do this as we document the fact that on DB engines that don't support this construct, we do not
   support/recommend running HA scheduler. If a user ignores this and tries anyway everything will still
   work, just slightly slower in some circumstances.

   Specifically don't emit NOWAIT for MySQL < 8, or MariaDB, neither of which support this construct

   See https://jira.mariadb.org/browse/MDEV-13115


.. function:: nulls_first(col, session: Session) -> Dict[str, Any]
   Adds a nullsfirst construct to the column ordering. Currently only Postgres supports it.
   In MySQL & Sqlite NULL values are considered lower than any non-NULL value, therefore, NULL values
   appear first when the order is ASC (ascending)


.. data:: USE_ROW_LEVEL_LOCKING
   :annotation: :bool

   

.. function:: with_row_locks(query, **kwargs)
   Apply with_for_update to an SQLAlchemy query, if row level locking is in use.

   :param query: An SQLAlchemy Query object
   :param **kwargs: Extra kwargs to pass to with_for_update (of, nowait, skip_locked, etc)
   :return: updated query


.. py:class:: CommitProhibitorGuard(session: Session)

   Context manager class that powers prohibit_commit

   .. attribute:: expected_commit
      :annotation: = False

      

   
   .. method:: _validate_commit(self, _)



   
   .. method:: __enter__(self)



   
   .. method:: __exit__(self, *exc_info)



   
   .. method:: commit(self)

      Commit the session.

      This is the required way to commit when the guard is in scope




.. function:: prohibit_commit(session)
   Return a context manager that will disallow any commit that isn't done via the context manager.

   The aim of this is to ensure that transaction lifetime is strictly controlled which is especially
   important in the core scheduler loop. Any commit on the session that is _not_ via this context manager
   will result in RuntimeError

   Example usage:

   .. code:: python

       with prohibit_commit(session) as guard:
           # ... do something with session
           guard.commit()

           # This would throw an error
           # session.commit()


.. function:: is_lock_not_available_error(error: OperationalError)
   Check if the Error is about not being able to acquire lock


