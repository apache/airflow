:mod:`airflow.providers.segment.hooks.segment`
==============================================

.. py:module:: airflow.providers.segment.hooks.segment

.. autoapi-nested-parse::

   This module contains a Segment Hook
   which allows you to connect to your Segment account,
   retrieve data from it or write to that file.

   NOTE:   this hook also relies on the Segment analytics package:
           https://github.com/segmentio/analytics-python



Module Contents
---------------

.. py:class:: SegmentHook(segment_conn_id: str = 'segment_default', segment_debug_mode: bool = False, *args, **kwargs)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Create new connection to Segment
   and allows you to pull data out of Segment or write to it.

   You can then use that file with other
   Airflow operators to move the data around or interact with segment.

   :param segment_conn_id: the name of the connection that has the parameters
       we need to connect to Segment. The connection should be type `json` and include a
       write_key security token in the `Extras` field.
   :type segment_conn_id: str
   :param segment_debug_mode: Determines whether Segment should run in debug mode.
       Defaults to False
   :type segment_debug_mode: bool

   .. note::
       You must include a JSON structure in the `Extras` field.
       We need a user's security token to connect to Segment.
       So we define it in the `Extras` field as:
       `{"write_key":"YOUR_SECURITY_TOKEN"}`

   
   .. method:: get_conn(self)



   
   .. method:: on_error(self, error: str, items: str)

      Handles error callbacks when using Segment with segment_debug_mode set to True




