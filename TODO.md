TODO
-----
#### UI
* Backfill form
* Add templating to adhoc queries
* Charts: better error handling

#### Command line
* Backfill, better logging, prompt with details about what tasks are about to run

#### More Operators!
* PIG
* MR
* Merge Cascading

#### Backend
* Add a run_only_latest flag to BaseOperator, runs only most recent task instance where deps are met
* Pickle all the THINGS!
* Distributed scheduler
* Raise errors when setting dependencies on task in foreign DAGs
* Add an is_test flag to the run context
* Add operator to task_instance table
* Add owner to task_instance table
* Add queued_dttm to task_instance table

#### Wishlist
* Support for cron like syntax (0 * * * ) using croniter library
* Pause flag at the task level
* Task callbacks as tasks?
* Increase unit test coverage
