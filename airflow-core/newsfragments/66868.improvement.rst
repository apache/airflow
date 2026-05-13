Speed up scheduler TaskInstance creation on PostgreSQL by switching the bulk INSERT
emitted by ``DagRun._create_task_instances`` to ``INSERT ... SELECT * FROM unnest(...)``.
The new form sends one typed array per column rather than one bind tuple per row, which
scales much better for large DagRuns (mapped task expansion, wide DAGs). Other database
backends are unaffected — they continue to use the ORM ``bulk_insert_mappings`` path.
