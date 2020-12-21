:mod:`airflow.cli.commands.connection_command`
==============================================

.. py:module:: airflow.cli.commands.connection_command

.. autoapi-nested-parse::

   Connection sub-commands



Module Contents
---------------

.. function:: _tabulate_connection(conns: List[Connection], tablefmt: str)

.. function:: _yamulate_connection(conn: Connection)

.. function:: connections_get(args)
   Get a connection.


.. function:: connections_list(args)
   Lists all connections at the command line


.. function:: _format_connections(conns: List[Connection], fmt: str) -> str

.. function:: _is_stdout(fileio: io.TextIOWrapper) -> bool

.. function:: _valid_uri(uri: str) -> bool
   Check if a URI is valid, by checking if both scheme and netloc are available


.. function:: connections_export(args)
   Exports all connections to a file


.. data:: alternative_conn_specs
   :annotation: = ['conn_type', 'conn_host', 'conn_login', 'conn_password', 'conn_schema', 'conn_port']

   

.. function:: connections_add(args)
   Adds new connection


.. function:: connections_delete(args)
   Deletes connection from DB


