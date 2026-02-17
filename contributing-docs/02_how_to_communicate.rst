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

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Various Communication channels
------------------------------

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
* GitHub `Pull Requests (PRs) <https://github.com/apache/airflow/pulls>`_ for:
   * discussing implementation details of PRs
   * not for architectural discussions (use the devlist for that)
* The deprecated `JIRA issues <https://issues.apache.org/jira/projects/AIRFLOW/issues/?filter=allopenissues&orderby=updated+DESC>`_ for:
    **IMPORTANT**
    We don't create new issues on JIRA anymore. The reason we still look at JIRA issues is that there are valuable
    tickets inside of it. However, each new PR should be created on `GitHub issues <https://github.com/apache/airflow/issues>`_
    as stated in `Contribution Workflow Example <contribution-workflow.rst>`_

   * checking out old but still valuable issues that are not on GitHub yet
   * mentioning the JIRA issue number in the title of the related PR you would like to open on GitHub


Slack details
-------------

* The `Apache Airflow Slack <https://s.apache.org/airflow-slack>`_ for:
   * ad-hoc questions related to development and asking for review (#contributors channel)
   * asking for help with first contribution PRs (#new-contributors channel)
   * troubleshooting (#user-troubleshooting channel)
   * using Breeze (#airflow-breeze channel)
   * improving and maintaining documentation (#documentation channel)
   * group talks (including SIG - special interest groups) (#sig-* channels)
   * notifications (#announcements channel)
   * random queries (#random channel)
   * regional announcements (#users-* channels)
   * occasional discussions (wherever appropriate including group and 1-1 discussions)

Please exercise caution against posting same questions across multiple channels. Doing so not only prevents
redundancy but also promotes more efficient and effective communication for everyone involved.

Devlist details
---------------

The devlist is the most important and official communication channel. Often at Apache project you can
hear "if it is not in the devlist - it did not happen". If you discuss and agree with someone from the
community on something important for the community (including if it is with maintainer or PMC member) the
discussion must be captured and re-shared on devlist in order to give other members of the community to
participate in it.

We are using certain prefixes for email subjects for different purposes. Start your email with one of those:
  * ``[DISCUSS]`` - if you want to discuss something but you have no concrete proposal yet
  * ``[PROPOSAL]`` - if usually after "[DISCUSS]" thread discussion you want to propose something and see
    what other members of the community think about it.
  * ``[AIP-NN]`` - if the mail is about one of the `Airflow Improvement Proposals <https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals>`_
  * ``[VOTE]`` - if you would like to start voting on a proposal discussed before in a "[PROPOSAL]" thread
  * ``[ANNOUNCE]`` - only used by PMC members to announce important things to the community such as
    releases or big changes in the project

Apart of regular discussions on development of the project, the community also appreciates if various
stakeholders, community members and participants share their experiences, case studies, best practices
as well as updates on what's going on in their organizations related to Apache Airflow.

There are certain rules for this kind of communication, it cannot be commercial in nature,
it should not promote specific products or services, and it should not suggest that the PMC or
the ASF endorse any specific products or services. The subject of such messages should refer to
Airflow in "Nominative fair use" way as described in
`Apache Trademark Policy <https://www.apache.org/foundation/marks/>`_.

Such messages can contain summary and digest of what happens in the community - as long as it is unbiased
and not commercial in nature. This kind of communication should be shared on devlist and
whoever sends such messages should be open to include information from other community members who
ask them to include their case studies, experiences or information.

Like all the communication in our project - such messages should follow the
`ASF Code of Conduct <https://www.apache.org/foundation/policies/conduct>`_.

If you are in doubt whether your message is appropriate for the devlist - ask the community on devlist.

**Voting**

Voting happens on the devlist and is governed by the rules
described in `Voting <https://www.apache.org/foundation/voting.html>`_

What to expect from the community
---------------------------------

We are all devoting our time for community as individuals who except for being active in Apache Airflow have
families, daily jobs, right for vacation. Sometimes we are in different timezones or simply are
busy with day-to-day duties that our response time might be delayed. For us it's crucial
to remember to respect each other in the project with no formal structure.
There are no managers, departments, most of us are autonomous in our opinions, decisions.
All of it makes Apache Airflow community a great space for open discussion and mutual respect
for various opinions.

Disagreements are expected, discussions might include strong opinions and contradicting statements.
Sometimes you might get two maintainers asking you to do things differently. This all happened in the past
and will continue to happen. As a community we have some mechanisms to facilitate discussion and come to
a consensus, conclusions or we end up voting to make important decisions. It is important that these
decisions are not treated as personal wins or losses. At the end it's the community that we all care about
and what's good for community, should be accepted even if you have a different opinion. There is a nice
motto that you should follow in case you disagree with community decision "Disagree but engage". Even
if you do not agree with a community decision, you should follow it and embrace (but you are free to
express your opinion that you don't agree with it).

As a community - we have high requirements for code quality. This is mainly because we are a distributed
and loosely organized team. We have both - contributors that commit one commit only, and people who add
more commits. It happens that some people assume informal "stewardship" over parts of code for some time -
but at any time we should make sure that the code can be taken over by others, without excessive communication.
Setting high requirements for the code (fairly strict code review, static code checks, requirements of
automated tests, prek hooks) is the best way to achieve that - by only accepting good quality
code. Thanks to full test coverage we can make sure that we will be able to work with the code in the future.
So do not be surprised if you are asked to add more tests or make the code cleaner -
this is for the sake of maintainability.

Rules for new contributors
--------------------------

Here are a few rules that are important to keep in mind when you enter our community:

* Do not be afraid to ask questions
* The communication is asynchronous - do not expect immediate answers, ping others on slack
  (#contributors channel) if blocked
* There is a #newbie-questions channel in slack as a safe place to ask questions
* You can ask one of the maintainers to be a mentor for you, maintainers can guide you within the community
* You can apply to more structured `Apache Mentoring Program <https://community.apache.org/mentoring/>`_
* It's your responsibility as an author to take your PR from start-to-end including leading communication
  in the PR
* It's your responsibility as an author to ping maintainers to review your PR - be mildly annoying sometimes,
  it's OK to be slightly annoying with your change - it is also a sign for maintainers that you care
* Be considerate to the high code quality/test coverage requirements for Apache Airflow
* If in doubt - ask the community for their opinion or propose to vote at the devlist
* Discussions should concern subject matters - judge or criticize the merit but never criticize people
* It's OK to express your own emotions while communicating - it helps other people to understand you
* Be considerate for feelings of others. Tell about how you feel not what you think of others

---------------

If you want to quick start your contribution, you can follow with
`Contributors Quick Start <03a_contributors_quick_start_beginners.rst>`__
