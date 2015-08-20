# Airflow

Airflow is a platform to programmatically author, schedule and monitor 
data pipelines.

When workflows are defined as code, they becomes more maintainable, 
versionable, testable, and collaborative.

![img] (http://i.imgur.com/6Gs4hxT.gif)

Use Airflow to author workflows as directed acyclic graphs (DAGs) of tasks. 
The Airflow scheduler executes your tasks on an array of workers while 
following the specified dependencies. Rich command line utilities make 
performing complex surgeries on DAGs a snap. The rich user interface 
makes it easy to visualize pipelines running in production, 
monitor progress, and troubleshoot issues when needed.

## Principles

- **Dynamic**:  Airflow pipelines are configuration as code (Python), allowing for dynamic pipeline generation. This allows for writing code that instantiates pipelines dynamically.
- **Extensible**:  Easily define your own operators, executors and extend the library so that it fits the level of abstraction that suits your environment.
- **Elegant**:  Airflow pipelines are lean and explicit. Parameterizing your scripts is built into the core of Airflow using the powerful **Jinja** templating engine.
- **Scalable**:  Airflow has a modular architecture and uses a message queue to orchestrate an arbitrary number of workers. Airflow is ready to scale to infinity.

## Who uses Airflow?

As the Airflow community grows, we'd like to keep track of who is using
the platform. Please send a PR with your company name and @githubhandle 
if you may.

Currently **officialy** using Airflow:

* Airbnb [@mistercrunch]
* Agari [@r39132]
* [Cotap](https://github.com/cotap/) [[@maraca](https://github.com/maraca) & [@richardchew](https://github.com/richardchew)]
* Lyft
* Stripe [@jbalogh]
* Wooga
* Xoom [[@gepser](https://github.com/gepser) & [@omarvides](https://github.com/omarvides)]
* Yahoo!

## Links 

* [Full documentation on pythonhosted.com](http://pythonhosted.org/airflow/)
* [Airflow Google Group (mailing list / forum)](https://groups.google.com/forum/#!forum/airbnb_airflow) 
* [Airbnb Blog Post about Airflow](http://nerds.airbnb.com/airflow/)
* [Hadoop Summit Airflow Video](https://www.youtube.com/watch?v=oYp49mBwH60)
