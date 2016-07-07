# Airflow

[![PyPI version](https://badge.fury.io/py/airflow.svg)](https://badge.fury.io/py/airflow)
[![Build Status](https://travis-ci.org/apache/incubator-airflow.svg)](https://travis-ci.org/apache/incubator-airflow)
[![Coverage Status](https://img.shields.io/codecov/c/github/apache/incubator-airflow/master.svg)](https://codecov.io/github/apache/incubator-airflow?branch=master)
[![Code Health](https://landscape.io/github/apache/incubator-airflow/master/landscape.svg?style=flat)](https://landscape.io/github/apache/incubator-airflow/master)
[![Requirements Status](https://requires.io/github/apache/incubator-airflow/requirements.svg?branch=master)](https://requires.io/github/apache/incubator-airflow/requirements/?branch=master)
[![Documentation](https://img.shields.io/badge/docs-pythonhosted-blue.svg)](http://pythonhosted.org/airflow/)
[![Join the chat at https://gitter.im/apache/incubator-airflow](https://badges.gitter.im/apache/incubator-airflow.svg)](https://gitter.im/apache/incubator-airflow?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Airflow is a platform to programmatically author, schedule and monitor
workflows.

When workflows are defined as code, they become more maintainable,
versionable, testable, and collaborative.

Use Airflow to author workflows as directed acyclic graphs (DAGs) of tasks.
The Airflow scheduler executes your tasks on an array of workers while
following the specified dependencies. Rich command line utilities make
performing complex surgeries on DAGs a snap. The rich user interface
makes it easy to visualize pipelines running in production,
monitor progress, and troubleshoot issues when needed.

## Getting started
Please visit the Airflow Platform documentation for help with [installing Airflow](http://pythonhosted.org/airflow/installation.html), getting a [quick start](http://pythonhosted.org/airflow/start.html), or a more complete [tutorial](http://pythonhosted.org/airflow/tutorial.html).

For further information, please visit the [Airflow Wiki](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Home).

## Beyond the Horizon

Airflow **is not** a data streaming solution. Tasks do not move data from
one to the other (though tasks can exchange metadata!). Airflow is not
in the [Spark Streaming](http://spark.apache.org/streaming/)
or [Storm](https://storm.apache.org/) space, it is more comparable to
[Oozie](http://oozie.apache.org/) or
[Azkaban](https://azkaban.github.io/).

Workflows are expected to be mostly static or slowly changing. You can think
of the structure of the tasks in your workflow as slightly more dynamic
than a database structure would be. Airflow workflows are expected to look
similar from a run to the next, this allows for clarity around
unit of work and continuity.

## Principles

- **Dynamic**:  Airflow pipelines are configuration as code (Python), allowing for dynamic pipeline generation. This allows for writing code that instantiates pipelines dynamically.
- **Extensible**:  Easily define your own operators, executors and extend the library so that it fits the level of abstraction that suits your environment.
- **Elegant**:  Airflow pipelines are lean and explicit. Parameterizing your scripts is built into the core of Airflow using the powerful **Jinja** templating engine.
- **Scalable**:  Airflow has a modular architecture and uses a message queue to orchestrate an arbitrary number of workers. Airflow is ready to scale to infinity.

## User Interface

- **DAGs**: Overview of all DAGs in your environment.
![](/docs/img/dags.png)

- **Tree View**: Tree representation of a DAG that spans across time.
![](/docs/img/tree.png)

- **Graph View**: Visualization of a DAG's dependencies and their current status for a specific run.
![](/docs/img/graph.png)

- **Task Duration**: Total time spent on different tasks over time.
![](/docs/img/duration.png)

- **Gantt View**: Duration and overlap of a DAG.
![](/docs/img/gantt.png)

- **Code View**:  Quick way to view source code of a DAG.
![](/docs/img/code.png)

## Who uses Airflow?

As the Airflow community grows, we'd like to keep track of who is using
the platform. Please send a PR with your company name and @githubhandle
if you may.

Committers:

* Refer to [Committers](https://cwiki.apache.org/confluence/display/AIRFLOW/Committers)

Currently **officially** using Airflow:

* [Airbnb](http://airbnb.io/) [[@mistercrunch](https://github.com/mistercrunch), [@artwr](https://github.com/artwr)]
* [Agari] (https://github.com/agaridata) [[@r39132](https://github.com/r39132)]
* [allegro.pl](http://allegro.tech/) [[@kretes](https://github.com/kretes)]
* [Bellhops](https://github.com/bellhops)
* BlueApron [[@jasonjho](https://github.com/jasonjho) & [@matthewdavidhauser](https://github.com/matthewdavidhauser)]
* [Clairvoyant] (https://clairvoyantsoft.com) [@shekharv](https://github.com/shekharv)
* [Clover Health] (https://www.cloverhealth.com) [[@gwax](https://github.com/gwax) & [@vansivallab](https://github.com/vansivallab)]
* Chartboost [[@cgelman](https://github.com/cgelman) & [@dclubb](https://github.com/dclubb)]
* [Cotap](https://github.com/cotap/) [[@maraca](https://github.com/maraca) & [@richardchew](https://github.com/richardchew)]
* Easy Taxi [[@caique-lima](https://github.com/caique-lima)]
* [FreshBooks](https://github.com/freshbooks) [[@DinoCow](https://github.com/DinoCow)]
* [Gentner Lab](http://github.com/gentnerlab) [[@neuromusic](https://github.com/neuromusic)]
* [Glassdoor](https://github.com/Glassdoor) [[@syvineckruyk](https://github.com/syvineckruyk)]
* [Handy](http://www.handy.com/careers/73115?gh_jid=73115&gh_src=o5qcxn) [[@marcintustin](https://github.com/marcintustin) / [@mtustin-handy](https://github.com/mtustin-handy)]
* [Holimetrix](http://holimetrix.com/) [[@thibault-ketterer](https://github.com/thibault-ketterer)]
* [Hootsuite](https://github.com/hootsuite)
* [ING](http://www.ing.com/)
* [Jampp](https://github.com/jampp)
* [Kiwi.com](https://kiwi.com/) [[@underyx](https://github.com/underyx)]
* [Kogan.com](https://github.com/kogan) [[@geeknam](https://github.com/geeknam)]
* [LendUp](https://www.lendup.com/) [[@lendup](https://github.com/lendup)]
* [LingoChamp](http://www.liulishuo.com/) [[@haitaoyao](https://github.com/haitaoyao)]
* [Lucid](http://luc.id) [[@jbrownlucid](https://github.com/jbrownlucid) & [@kkourtchikov](https://github.com/kkourtchikov)]
* [Lyft](https://www.lyft.com/)[[@SaurabhBajaj](https://github.com/SaurabhBajaj)]
* [Nerdwallet](https://www.nerdwallet.com)
* [Postmates](http://www.postmates.com) [[@syeoryn](https://github.com/syeoryn)]
* [Qubole](https://qubole.com) [[@msumit](https://github.com/msumit)]
* [Sense360](https://github.com/Sense360) [[@kamilmroczek](https://github.com/KamilMroczek)]
* [Sidecar](https://hello.getsidecar.com/) [[@getsidecar](https://github.com/getsidecar)]
* [SimilarWeb](https://www.similarweb.com/) [[@similarweb](https://github.com/similarweb)]
* [SmartNews](https://www.smartnews.com/) [[@takus](https://github.com/takus)]
* Stripe [@jbalogh]
* [Thumbtack](https://www.thumbtack.com/) [[@natekupp](https://github.com/natekupp)]
* [WeTransfer](https://github.com/WeTransfer) [[@jochem](https://github.com/jochem)]
* Wooga
* Xoom [[@gepser](https://github.com/gepser) & [@omarvides](https://github.com/omarvides)]
* [WePay](http://www.wepay.com) [[@criccomini](https://github.com/criccomini) & [@mtagle](https://github.com/mtagle)]
* Yahoo!
* [Zendesk](https://www.github.com/zendesk)

## Links

* [Full documentation on pythonhosted.org](http://pythonhosted.org/airflow/)
* [Airflow Apache (incubating) (mailing list)](http://mail-archives.apache.org/mod_mbox/incubator-airflow-dev/)
* [Airbnb Blog Post about Airflow](http://nerds.airbnb.com/airflow/)
* [Airflow Common Pitfalls](https://cwiki.apache.org/confluence/display/AIRFLOW/Common+Pitfalls)
* [Hadoop Summit Airflow Video](https://www.youtube.com/watch?v=oYp49mBwH60)
* [Airflow at Agari Blog Post](http://agari.com/blog/airflow-agari)
* [Best practices with Airflow (Max) nov 2015](https://youtu.be/dgaoqOZlvEA)
* [Airflow (Lesson 1) : TriggerDagRunOperator](https://www.linkedin.com/pulse/airflow-lesson-1-triggerdagrunoperator-siddharth-anand?published=t)
* [Docker Airflow (externally maintained)](https://github.com/puckel/docker-airflow)
* [Airflow: Tips, Tricks, and Pitfalls @ Handy](https://medium.com/handy-tech/airflow-tips-tricks-and-pitfalls-9ba53fba14eb#.o2snqeoz7)
* Airflow Chef recipe (community contributed) [github] (https://github.com/bahchis/airflow-cookbook) [chef] (https://supermarket.chef.io/cookbooks/airflow)
* Airflow Puppet Module (community contributed) [github] (https://github.com/similarweb/puppet-airflow) [puppet forge] (https://forge.puppetlabs.com/similarweb/airflow)
* [Gitter (live chat) Channel](https://gitter.im/apache/incubator-airflow)
