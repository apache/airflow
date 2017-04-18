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

1. [Airbnb](http://airbnb.io/) [[@mistercrunch](https://github.com/mistercrunch), [@artwr](https://github.com/artwr)]
1. [Agari](https://github.com/agaridata) [[@r39132](https://github.com/r39132)]
1. [allegro.pl](http://allegro.tech/) [[@kretes](https://github.com/kretes)]
1. [AltX](https://www.getaltx.com/about) [[@pedromduarte](https://github.com/pedromduarte)]
1. [Apigee](https://apigee.com) [[@btallman](https://github.com/btallman)]
1. [Astronomer](http://www.astronomer.io) [[@schnie](https://github.com/schnie)]
1. [Auth0](https://auth0.com) [[@sicarul](https://github.com/sicarul)]
1. [BandwidthX](http://www.bandwidthx.com) [[@dineshdsharma](https://github.com/dineshdsharma)]
1. [Bellhops](https://github.com/bellhops)
1. [BlaBlaCar](https://www.blablacar.com) [[@puckel](https://github.com/puckel) & [@wmorin](https://github.com/wmorin)]
1. [Bloc](https://www.bloc.io) [[@dpaola2](https://github.com/dpaola2)]
1. BlueApron [[@jasonjho](https://github.com/jasonjho) & [@matthewdavidhauser](https://github.com/matthewdavidhauser)]
1. [Blue Yonder](http://www.blue-yonder.com) [[@blue-yonder](https://github.com/blue-yonder)]
1. [Celect](http://www.celect.com) [[@superdosh](https://github.com/superdosh) & [@chadcelect](https://github.com/chadcelect)]
1. [Change.org](https://www.change.org) [[@change](https://github.com/change), [@vijaykramesh](https://github.com/vijaykramesh)]
1. [Children's Hospital of Philadelphia Division of Genomic Diagnostics](http://www.chop.edu/centers-programs/division-genomic-diagnostics) [[@genomics-geek]](https://github.com/genomics-geek/)
1. [City of San Diego](http://sandiego.gov) [[@MrMaksimize](https://github.com/mrmaksimize), [@andrell81](https://github.com/andrell81) & [@arnaudvedy](https://github.com/arnaudvedy)]
1. [Clairvoyant](https://clairvoyantsoft.com) [@shekharv](https://github.com/shekharv)
1. [Clover Health](https://www.cloverhealth.com) [[@gwax](https://github.com/gwax) & [@vansivallab](https://github.com/vansivallab)]
1. Chartboost [[@cgelman](https://github.com/cgelman) & [@dclubb](https://github.com/dclubb)]
1. [Cotap](https://github.com/cotap/) [[@maraca](https://github.com/maraca) & [@richardchew](https://github.com/richardchew)]
1. [Digital First Media](http://www.digitalfirstmedia.com/) [[@duffn](https://github.com/duffn) & [@mschmo](https://github.com/mschmo) & [@seanmuth](https://github.com/seanmuth)]
1. [Easy Taxi](http://www.easytaxi.com/) [[@caique-lima](https://github.com/caique-lima) & [@WesleyBatista](https://github.com/WesleyBatista)]
1. [evo.company](https://evo.company/) [[@orhideous](https://github.com/orhideous)]
1. [FreshBooks](https://github.com/freshbooks) [[@DinoCow](https://github.com/DinoCow)]
1. [Gentner Lab](http://github.com/gentnerlab) [[@neuromusic](https://github.com/neuromusic)]
1. [Glassdoor](https://github.com/Glassdoor) [[@syvineckruyk](https://github.com/syvineckruyk)]
1. [GovTech GDS](https://gds-gov.tech) [[@chrissng](https://github.com/chrissng) & [@datagovsg](https://github.com/datagovsg)]
1. [Groupalia](http://es.groupalia.com) [[@jesusfcr](https://github.com/jesusfcr)]
1. [Gusto](https://gusto.com) [[@frankhsu](https://github.com/frankhsu)]
1. [Handshake](https://joinhandshake.com/) [[@mhickman](https://github.com/mhickman)]
1. [Handy](http://www.handy.com/careers/73115?gh_jid=73115&gh_src=o5qcxn) [[@marcintustin](https://github.com/marcintustin) / [@mtustin-handy](https://github.com/mtustin-handy)]
1. [HBO](http://www.hbo.com/)[[@yiwang](https://github.com/yiwang)]
1. [HelloFresh](https://www.hellofresh.com) [[@tammymendt](https://github.com/tammymendt) & [@davidsbatista](https://github.com/davidsbatista) & [@iuriinedostup](https://github.com/iuriinedostup)]
1. [Holimetrix](http://holimetrix.com/) [[@thibault-ketterer](https://github.com/thibault-ketterer)]
1. [Hootsuite](https://github.com/hootsuite)
1. [IFTTT](https://www.ifttt.com/) [[@apurvajoshi](https://github.com/apurvajoshi)]
1. [iHeartRadio](http://www.iheart.com/)[[@yiwang](https://github.com/yiwang)]
1. [ING](http://www.ing.com/)
1. [Jampp](https://github.com/jampp)
1. [Kiwi.com](https://kiwi.com/) [[@underyx](https://github.com/underyx)]
1. [Kogan.com](https://github.com/kogan) [[@geeknam](https://github.com/geeknam)]
1. [Lemann Foundation](http://fundacaolemann.org.br) [[@fernandosjp](https://github.com/fernandosjp)]
1. [LendUp](https://www.lendup.com/) [[@lendup](https://github.com/lendup)]
1. [Letsbonus](http://www.letsbonus.com) [[@jesusfcr](https://github.com/jesusfcr)]
1. [liligo](http://liligo.com/) [[@tromika](https://github.com/tromika)]
1. [LingoChamp](http://www.liulishuo.com/) [[@haitaoyao](https://github.com/haitaoyao)]
1. [Lucid](http://luc.id) [[@jbrownlucid](https://github.com/jbrownlucid) & [@kkourtchikov](https://github.com/kkourtchikov)]
1. [Lumos Labs](https://www.lumosity.com/) [[@rfroetscher](https://github.com/rfroetscher/) & [@zzztimbo](https://github.com/zzztimbo/)]
1. [Lyft](https://www.lyft.com/)[[@SaurabhBajaj](https://github.com/SaurabhBajaj)]
1. [Madrone](http://madroneco.com/) [[@mbreining](https://github.com/mbreining) & [@scotthb](https://github.com/scotthb)]
1. [Markovian](https://markovian.com/) [[@al-xv](https://github.com/al-xv), [@skogsbaeck](https://github.com/skogsbaeck), [@waltherg](https://github.com/waltherg)]
1. [Mercadoni](https://www.mercadoni.com.co) [[@demorenoc](https://github.com/demorenoc)]
1. [MiNODES](https://www.minodes.com) [[@dice89](https://github.com/dice89), [@diazcelsa](https://github.com/diazcelsa)]
1. [MFG Labs](https://github.com/MfgLabs)
1. [mytaxi](https://mytaxi.com) [[@mytaxi](https://github.com/mytaxi)]
1. [Nerdwallet](https://www.nerdwallet.com)
1. [OfferUp](https://offerupnow.com)
1. [OneFineStay](https://www.onefinestay.com) [[@slangwald](https://github.com/slangwald)]
1. [Open Knowledge International](https://okfn.org) [@vitorbaptista](https://github.com/vitorbaptista)
1. [PayPal](https://www.paypal.com/) [[@jhsenjaliya](https://github.com/jhsenjaliya)]
1. [Postmates](http://www.postmates.com) [[@syeoryn](https://github.com/syeoryn)]
1. [Qubole](https://qubole.com) [[@msumit](https://github.com/msumit)]
1. [Scaleway](https://scaleway.com) [[@kdeldycke](https://github.com/kdeldycke)]
1. [Sense360](https://github.com/Sense360) [[@kamilmroczek](https://github.com/KamilMroczek)]
1. [Shopkick](https://shopkick.com/) [[@shopkick](https://github.com/shopkick)]
1. [Sidecar](https://hello.getsidecar.com/) [[@getsidecar](https://github.com/getsidecar)]
1. [SimilarWeb](https://www.similarweb.com/) [[@similarweb](https://github.com/similarweb)]
1. [SmartNews](https://www.smartnews.com/) [[@takus](https://github.com/takus)]
1. [Spotify](https://github.com/spotify) [[@znichols](https://github.com/znichols)]
1. [Stackspace](https://beta.stackspace.io/)
1. Stripe [[@jbalogh](https://github.com/jbalogh)]
1. [Thumbtack](https://www.thumbtack.com/) [[@natekupp](https://github.com/natekupp)]
1. [T2 Systems](http://t2systems.com) [[@unclaimedpants](https://github.com/unclaimedpants)]
1. [Vente-Exclusive.com](http://www.vente-exclusive.com/) [[@alexvanboxel](https://github.com/alexvanboxel)]
1. [Vnomics](https://github.com/vnomics) [[@lpalum](https://github.com/lpalum)]
1. [WePay](http://www.wepay.com) [[@criccomini](https://github.com/criccomini) & [@mtagle](https://github.com/mtagle)]
1. [WeTransfer](https://github.com/WeTransfer) [[@jochem](https://github.com/jochem)]
1. [Whistle Labs](http://www.whistle.com) [[@ananya77041](https://github.com/ananya77041)]
1. [WiseBanyan](https://wisebanyan.com/)
1. Wooga
1. Xoom [[@gepser](https://github.com/gepser) & [@omarvides](https://github.com/omarvides)]
1. Yahoo!
1. [Zapier](https://www.zapier.com) [[@drknexus](https://github.com/drknexus) & [@statwonk](https://github.com/statwonk)]
1. [Zendesk](https://www.github.com/zendesk)
1. [Zenly](https://zen.ly) [[@cerisier](https://github.com/cerisier) & [@jbdalido](https://github.com/jbdalido)]
1. [99](https://99taxis.com) [[@fbenevides](https://github.com/fbenevides), [@gustavoamigo](https://github.com/gustavoamigo) & [@mmmaia](https://github.com/mmmaia)]

## Links


* [Documentation](http://airflow.incubator.apache.org/)
* [Chat](https://gitter.im/apache/incubator-airflow)
* [Apache Airflow Incubation Status](http://incubator.apache.org/projects/airflow.html)
* [More](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Links)
