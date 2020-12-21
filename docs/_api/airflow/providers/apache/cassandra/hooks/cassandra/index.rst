:mod:`airflow.providers.apache.cassandra.hooks.cassandra`
=========================================================

.. py:module:: airflow.providers.apache.cassandra.hooks.cassandra

.. autoapi-nested-parse::

   This module contains hook to integrate with Apache Cassandra.



Module Contents
---------------

.. data:: Policy
   

   

.. py:class:: CassandraHook(cassandra_conn_id: str = 'cassandra_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Hook used to interact with Cassandra

   Contact points can be specified as a comma-separated string in the 'hosts'
   field of the connection.

   Port can be specified in the port field of the connection.

   If SSL is enabled in Cassandra, pass in a dict in the extra field as kwargs for
   ``ssl.wrap_socket()``. For example::

       {
           'ssl_options' : {
               'ca_certs' : PATH_TO_CA_CERTS
           }
       }

   Default load balancing policy is RoundRobinPolicy. To specify a different
   LB policy::

       - DCAwareRoundRobinPolicy
           {
               'load_balancing_policy': 'DCAwareRoundRobinPolicy',
               'load_balancing_policy_args': {
                   'local_dc': LOCAL_DC_NAME,                      // optional
                   'used_hosts_per_remote_dc': SOME_INT_VALUE,     // optional
               }
            }
       - WhiteListRoundRobinPolicy
           {
               'load_balancing_policy': 'WhiteListRoundRobinPolicy',
               'load_balancing_policy_args': {
                   'hosts': ['HOST1', 'HOST2', 'HOST3']
               }
           }
       - TokenAwarePolicy
           {
               'load_balancing_policy': 'TokenAwarePolicy',
               'load_balancing_policy_args': {
                   'child_load_balancing_policy': CHILD_POLICY_NAME, // optional
                   'child_load_balancing_policy_args': { ... }       // optional
               }
           }

   For details of the Cluster config, see cassandra.cluster.

   
   .. method:: get_conn(self)

      Returns a cassandra Session object



   
   .. method:: get_cluster(self)

      Returns Cassandra cluster.



   
   .. method:: shutdown_cluster(self)

      Closes all sessions and connections associated with this Cluster.



   
   .. staticmethod:: get_lb_policy(policy_name: str, policy_args: Dict[str, Any])

      Creates load balancing policy.

      :param policy_name: Name of the policy to use.
      :type policy_name: str
      :param policy_args: Parameters for the policy.
      :type policy_args: Dict



   
   .. method:: table_exists(self, table: str)

      Checks if a table exists in Cassandra

      :param table: Target Cassandra table.
                    Use dot notation to target a specific keyspace.
      :type table: str



   
   .. method:: record_exists(self, table: str, keys: Dict[str, str])

      Checks if a record exists in Cassandra

      :param table: Target Cassandra table.
                    Use dot notation to target a specific keyspace.
      :type table: str
      :param keys: The keys and their values to check the existence.
      :type keys: dict




