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

Limit on open Pull Requests
===========================

Contributors without write access to the ``apache/airflow`` repository can have at most
**5 open Pull Requests at a time**. This page explains why we introduced the limit, how it works,
what happens to Pull Requests that were already open when it was introduced, and what you can do
when you reach it.

.. contents:: :local:

Why we limit open Pull Requests
-------------------------------

Reviewer attention is the scarcest resource in the project. When we had around 100 open Pull
Requests, we could promise - and mostly delivered - at least an initial review for each of them.
With more than 1,000 open Pull Requests, that promise is impossible to keep, and hundreds of Pull
Requests sit without any review at all. Opening more Pull Requests does not make any of them get
merged faster - it only makes the queue longer for everyone.

Generating code has also become fast. A submitted Pull Request no longer shows on its own
that its author understands Airflow or is invested in the change, and deciding whether a Pull Request
deserves a deeper review takes time that maintainers do not have for a queue of that size.

The limit is part of a social contract we want to build:

* **Maintainers** commit to reviewing Pull Requests thoroughly and seriously, from a queue that is
  small enough to do that.
* **Contributors** prioritize their work - they choose which changes matter most to them, and they
  help the project in other ways while their Pull Requests are waiting for review.

The limit was agreed by the community on the ``dev@airflow.apache.org`` mailing list in the
"Limiting number of opened pull requests for users without write access" lazy consensus thread.
Five is a starting point: we will watch how the queue develops and revisit the number if needed.

How the limit works
-------------------

* The limit uses GitHub's
  `pull request limit for users without write access <https://docs.github.com/en/communities/moderating-comments-and-conversations/limiting-interactions-in-your-repository#limiting-concurrent-open-pull-requests-for-users-without-write-access>`__,
  enabled in the repository's ``.asf.yaml`` file.
* It applies to every user **without write access** to the repository.
* Draft Pull Requests do not count towards the limit yet, because GitHub does not support counting
  them. Once GitHub adds that, we will start counting drafts as well - so do not rely on drafts to
  get around the limit. Keep unfinished work on a branch in your fork instead.
* When you have 5 open Pull Requests, GitHub does not let you open another one. A slot frees up when
  one of your Pull Requests is merged or closed.
* The limit counts Pull Requests you have open at the same time - there is no limit on how many
  Pull Requests you contribute over time.

What happens to Pull Requests that were already open
----------------------------------------------------

GitHub's limit only stops new Pull Requests from being opened - it does not close anything. To bring
the queue in line with the limit, maintainers run a **one-time closure** when the limit is
introduced:

1. The limit is announced on the ``dev@airflow.apache.org`` mailing list, together with a link to
   this page.
2. Maintainers close the open Pull Requests of every contributor without write access who has more
   than 5 open Pull Requests at that time - counted the same way as GitHub counts them, so drafts do
   not count yet.
3. Pull Requests where a maintainer is already engaged - has commented on or reviewed the Pull
   Request - stay open, so that review work already invested is not thrown away. Automated triage
   comments do not count as engagement. Pull Requests that stay open still count towards your limit.
4. All the other open Pull Requests of that contributor are closed - drafts included - and labeled
   ``closed because of open PR limit``. Each of them gets a comment listing which of your Pull
   Requests were closed and which stayed open, with a link to this page.

The closure is done with the ``dev/close_prs_over_open_pr_limit.py`` script, so that every
contributor over the limit is treated the same way.

This is not a judgement of you or of your changes. We never told contributors before that opening
many Pull Requests at once was a problem, so there is nothing to feel bad about. Nothing is lost
either - your branches and commits stay where they are (see
`Reopening a closed Pull Request`_ below).

What we ask you to do is to make your **first prioritization decision**: choose the Pull Requests that
matter most to you and reopen them - up to 5 open at a time, including the ones that stayed open.
Reopen the ones you are ready to follow through - keep them rebased, respond to review comments and
fix failing checks. Maintainers do pay attention to which Pull Requests contributors choose to reopen
and how they engage afterwards.

What to do when you reach the limit
-----------------------------------

Reaching the limit is a signal to focus, not a signal to stop contributing.

Get your open Pull Requests over the line:

* Respond to review comments, resolve conversations and push the requested changes.
* Rebase on the latest ``main`` and fix failing checks, so that the Pull Request is ready to merge.
* Close Pull Requests you no longer intend to finish - that frees a slot for something that matters
  more to you.

Keep working on your next changes:

* Nothing stops you from working on branches in your fork - you just cannot open a Pull Request for
  them yet. Open the Pull Request once a slot frees up.

Help the project in other ways - these contributions are often more valuable than more Pull Requests,
and they are what maintainers look at when deciding who to invite as a committer:

* **Review other people's Pull Requests.** Every review from a contributor who knows the area makes
  the maintainers' review faster.
* Help triage and reproduce issues, and answer questions from users.
* Join the discussions on the ``dev@airflow.apache.org`` mailing list and in the dev calls.
* Help other contributors on `Slack <https://s.apache.org/airflow-slack>`__, for example in the
  ``#new-contributors`` channel.

See `How to contribute <04_how_to_contribute.rst>`__ and
`How to communicate <02_how_to_communicate.rst>`__ for more ways to get involved.

Reopening a closed Pull Request
-------------------------------

Closing a Pull Request does not delete anything. The branch in your fork and all of its commits stay
where they were, and the Pull Request keeps its whole review history.

* Once you have a free slot, reopen the Pull Request with the **Reopen pull request** button at the
  bottom of the Pull Request page, or with the GitHub CLI:

  .. code-block:: bash

      gh pr reopen <PR_NUMBER> --repo apache/airflow

* If the Pull Request cannot be reopened - for example because you force-pushed to or deleted its
  branch after it was closed - push the branch again (under a new name if needed) and open a new Pull
  Request from it. Link the old Pull Request in the description so reviewers can find the earlier
  discussion.
* If you deleted the branch in your fork, you can restore the commits from the closed Pull Request:

  .. code-block:: bash

      git fetch upstream pull/<PR_NUMBER>/head:<BRANCH_NAME>
      git push origin <BRANCH_NAME>

  This assumes ``upstream`` points at ``apache/airflow`` and ``origin`` at your fork - see
  `Working with Git <10_working_with_git.rst>`__.

Exceptions
----------

GitHub supports a bypass list of users who are exempt from the limit. Support for configuring it
through ``.asf.yaml`` is being added in
`apache/infrastructure-asfyaml#135 <https://github.com/apache/infrastructure-asfyaml/pull/135>`__.
Until it is available, there are no exceptions. Once it is, maintainers may add contributors to the
list where it makes sense - for example to let someone open an urgent fix while they are at the
limit. If you think you need an exception, ask on the ``dev@airflow.apache.org`` mailing list.

If you believe the limit was applied to you in error - for example your Pull Requests were closed by
mistake - the appeal channel is the PMC private list; see the
`Community escalation process <../COMMUNITY_ESCALATION.md>`__.
