#### UI
* Backfill form
* Better task filtering int duration and landing time charts (operator toggle, task regex, uncheck all button)
* Add templating to adhoc queries

#### Backend
* Add a run_only_latest flag to BaseOperator, runs only most recent task instance where deps are met
* Pickle all the THINGS!
* Distributed scheduler
* Raise errors when setting dependencies on task in foreign DAGs
* Add an is_test flag to the run context
* Add operator to task_instance table

#### Wishlist
* Pause flag at the task level
* Increase unit test coverage

#### Other
* deprecate TimeSensor
