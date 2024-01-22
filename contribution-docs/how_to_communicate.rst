 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

How to communicate
==================

Apache Airflow is a Community within Apache Software Foundation. As the motto of
the Apache Software Foundation states "Community over Code" - people in the
community are far more important than their contribution.

This means that communication plays a big role in it, and this chapter is all about it.

In our communication, everyone is expected to follow the `ASF Code of Conduct <https://www.apache.org/foundation/policies/conduct>`_.

We have various channels of communication - starting from the official devlist, comments
in the PR, Slack, wiki.

All those channels can be used for different purposes.
You can join the channels via links at the `Airflow Community page <https://airflow.apache.org/community/>`_

* The `Apache Airflow devlist <https://lists.apache.org/list.html?dev@airflow.apache.org>`_ for:
   * official communication
   * general issues, asking community for opinion
   * discussing proposals
   * voting
* The `Airflow CWiki <https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Home?src=breadcrumbs>`_ for:
   * detailed discussions on big proposals (Airflow Improvement Proposals also name AIPs)
   * helpful, shared resources (for example Apache Airflow logos
   * information that can be reused by others (for example instructions on preparing workshops)
* GitHub `Pull Requests (PRs) <https://github.com/apache/airflow/pulls>`_ for:
   * discussing implementation details of PRs
   * not for architectural discussions (use the devlist for that)
* The deprecated `JIRA issues <https://issues.apache.org/jira/projects/AIRFLOW/issues/AIRFLOW-4470?filter=allopenissues>`_ for:
   * checking out old but still valuable issues that are not on GitHub yet
   * mentioning the JIRA issue number in the title of the related PR you would like to open on GitHub

**IMPORTANT**
We don't create new issues on JIRA anymore. The reason we still look at JIRA issues is that there are valuable
tickets inside of it. However, each new PR should be created on `GitHub issues <https://github.com/apache/airflow/issues>`_
as stated in `Contribution Workflow Example <contribution-workflow.rst>`_

* The `Apache Airflow Slack <https://s.apache.org/airflow-slack>`_ for:
   * ad-hoc questions related to development (#development channel)
   * asking for review (#development channel)
   * asking for help with first contribution PRs (#development-first-pr-support channel)
   * troubleshooting (#troubleshooting channel)
   * group talks (including SIG - special interest groups) (#sig-* channels)
   * notifications (#announcements channel)
   * random queries (#random channel)
   * regional announcements (#users-* channels)
   * occasional discussions (wherever appropriate including group and 1-1 discussions)

Please exercise caution against posting same questions across multiple channels. Doing so not only prevents
redundancy but also promotes more efficient and effective communication for everyone involved.

The devlist is the most important and official communication channel. Often at Apache project you can
hear "if it is not in the devlist - it did not happen". If you discuss and agree with someone from the
community on something important for the community (including if it is with maintainer or PMC member) the
discussion must be captured and reshared on devlist in order to give other members of the community to
participate in it.

We are using certain prefixes for email subjects for different purposes. Start your email with one of those:
  * ``[DISCUSS]`` - if you want to discuss something but you have no concrete proposal yet
  * ``[PROPOSAL]`` - if usually after "[DISCUSS]" thread discussion you want to propose something and see
    what other members of the community think about it.
  * ``[AIP-NN]`` - if the mail is about one of the Airflow Improvement Proposals
  * ``[VOTE]`` - if you would like to start voting on a proposal discussed before in a "[PROPOSAL]" thread

Voting is governed by the rules described in `Voting <https://www.apache.org/foundation/voting.html>`_

We are all devoting our time for community as individuals who except for being active in Apache Airflow have
families, daily jobs, right for vacation. Sometimes we are in different timezones or simply are
busy with day-to-day duties that our response time might be delayed. For us it's crucial
to remember to respect each other in the project with no formal structure.
There are no managers, departments, most of us is autonomous in our opinions, decisions.
All of it makes Apache Airflow community a great space for open discussion and mutual respect
for various opinions.

Disagreements are expected, discussions might include strong opinions and contradicting statements.
Sometimes you might get two maintainers asking you to do things differently. This all happened in the past
and will continue to happen. As a community we have some mechanisms to facilitate discussion and come to
a consensus, conclusions or we end up voting to make important decisions. It is important that these
decisions are not treated as personal wins or looses. At the end it's the community that we all care about
and what's good for community, should be accepted even if you have a different opinion. There is a nice
motto that you should follow in case you disagree with community decision "Disagree but engage". Even
if you do not agree with a community decision, you should follow it and embrace (but you are free to
express your opinion that you don't agree with it).

As a community - we have high requirements for code quality. This is mainly because we are a distributed
and loosely organised team. We have both - contributors that commit one commit only, and people who add
more commits. It happens that some people assume informal "stewardship" over parts of code for some time -
but at any time we should make sure that the code can be taken over by others, without excessive communication.
Setting high requirements for the code (fairly strict code review, static code checks, requirements of
automated tests, pre-commit checks) is the best way to achieve that - by only accepting good quality
code. Thanks to full test coverage we can make sure that we will be able to work with the code in the future.
So do not be surprised if you are asked to add more tests or make the code cleaner -
this is for the sake of maintainability.

Here are a few rules that are important to keep in mind when you enter our community:

* Do not be afraid to ask questions
* The communication is asynchronous - do not expect immediate answers, ping others on slack
  (#development channel) if blocked
* There is a #newbie-questions channel in slack as a safe place to ask questions
* You can ask one of the maintainers to be a mentor for you, maintainers can guide you within the community
* You can apply to more structured `Apache Mentoring Programme <https://community.apache.org/mentoringprogramme.html>`_
* It's your responsibility as an author to take your PR from start-to-end including leading communication
  in the PR
* It's your responsibility as an author to ping maintainers to review your PR - be mildly annoying sometimes,
  it's OK to be slightly annoying with your change - it is also a sign for maintainers that you care
* Be considerate to the high code quality/test coverage requirements for Apache Airflow
* If in doubt - ask the community for their opinion or propose to vote at the devlist
* Discussions should concern subject matters - judge or criticise the merit but never criticise people
* It's OK to express your own emotions while communicating - it helps other people to understand you
* Be considerate for feelings of others. Tell about how you feel not what you think of others
