#### Roadmap items
* UI page answering "Why isn't this task instance running?"
* Attempt removing DagBag caching for the web server
* Distributed scheduler (supervisors)
    * Get the supervisors to run sensors (as opposed to each sensor taking a slot)
    * Improve DagBag differential refresh
    * Pickle all the THINGS! supervisors maintains fresh, versionned pickles in the database as they monitor for change
* Pre-prod running off of master
* Containment / YarnExecutor / Docker?
* Get s3 logs
* Test and migrate to use beeline instead of the Hive CLI
* Run Hive / Hadoop / HDFS tests in Travis-CI

#### UI
* Backfill form
* Better task filtering int duration and landing time charts (operator toggle, task regex, uncheck all button)
* Add templating to adhoc queries

#### Backend
* Add a run_only_latest flag to BaseOperator, runs only most recent task instance where deps are met
* Raise errors when setting dependencies on task in foreign DAGs
* Add an is_test flag to the run context

#### Wishlist
* Pause flag at the task level
* Increase unit test coverage
* Stats logging interface with support for stats and sqlalchemy to collect detailed information from the scheduler and dag processing times

#### Other
* deprecate TimeSensor
