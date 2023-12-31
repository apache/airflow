Support for Microsoft SQL-Server for Airflow Meta Database has been removed

After `discussion <https://lists.apache.org/thread/r06j306hldg03g2my1pd4nyjxg78b3h4>`__
and a `voting process <https://lists.apache.org/thread/pgcgmhf6560k8jbsmz8nlyoxosvltph2>`__,
the Airflow's PMC and Committers have reached a resolution to no longer maintain MsSQL as a supported Database Backend.

As of Airflow 2.9.0 support of MsSQL has been removed for Airflow Database Backend.

A migration script which can help migrating the database *before* upgrading to Airflow 2.9.0 is available in
`airflow-mssql-migration repo on Github <https://github.com/apache/airflow-mssql-migration>`_.
Note that the migration script is provided without support and warranty.

This does not affect the existing provider packages (operators and hooks), DAGs can still access and process data from MsSQL.
