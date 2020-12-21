:mod:`airflow.utils.weight_rule`
================================

.. py:module:: airflow.utils.weight_rule


Module Contents
---------------

.. py:class:: WeightRule

   Weight rules.

   .. attribute:: DOWNSTREAM
      :annotation: = downstream

      

   .. attribute:: UPSTREAM
      :annotation: = upstream

      

   .. attribute:: ABSOLUTE
      :annotation: = absolute

      

   .. attribute:: _ALL_WEIGHT_RULES
      :annotation: :Set[str]

      

   
   .. classmethod:: is_valid(cls, weight_rule)

      Check if weight rule is valid.



   
   .. classmethod:: all_weight_rules(cls)

      Returns all weight rules




