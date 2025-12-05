Partitioning
============

.. node::

  This is a working doc that will be updated as AIP-76 is implemented.

Dags can be set to run for partitions.

Currently we an set a dag to be scheduled on updates of partitioned assets.

This can be 1-1 or with a partition mappping, so that the downstream will wait for the completeness
of its upstream partitions before running.

Next things to look at:

  - partition scheduling interface (currently depends on manual triggering)
  - composite partitions
  - what to do about logical date
    - i.e. should partitions be derived from it or not?
    - if they are, how do we work it so we have opt in?
    - i.e. we don't want all asset updates to automatically be partition-aware now. so how
      does the user signal that they want logical date to be used as partition key.
    - i would prefer a hard distinction between logical date and partition
  - segments

Deferred past 3.2:

  - partitioning by segments

We need to ensure that triggering asset events works for partition-driven dags too.

Watchers -- partitioning


