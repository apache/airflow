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

Documentation
#############

This directory contains documentation for the `Apache Airflow project <https://airflow.apache.org/docs/>`__ and the providers packages that are closely related to it. You can contribute to the Airflow Docs in the same way and for the same reasons as contributing code; Docs contributions that improve existing content, fix bugs, and create new content are welcomed and encouraged.

This README gives an overview about how Airflow uses `Sphinx <https://www.sphinx-doc.org/>`__ to write and build docs. It also includes instructions on how to make Docs changes locally or with the GitHub UI.

Development documentation preview
==================================

You can find documentation for the current development version at `s.apache.org/airflow-docs <https://s.apache.org/airflow-docs>`_, where it is automatically built and published.

Working with Sphinx
===================

Airflow Documentation uses `Sphinx <https://www.sphinx-doc.org/>`__, a reStructure Text (.rst) markup language that was developed to document Python and Python projects, to build the docs site.

For most Docs writing purposes, the `reStructured Text Primer <https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html>`__ provides a quick reference of common formatting and syntax.

A general docs workflow
-----------------------
When you make changes to the docs, it follows roughly the same process as creating and testing code changes. However, for docs, instead of unit tests and integration tests, you run pre-commit checks, spell checks, and visual inspection of your changes in a documentation build.

1. **Decide to edit in GitHub UI or locally** - Depending on the size and type of docs update you want to make, it might be easier to work in the UI or to make your edits in a local fork.
2. **Find the source files to edit** - While you can access most of the docs source files using the **Suggest a change on this page** button or by searching for a string in the ``/docs/`` file directory, in some cases, the source strings might be located in different provider docs or in the source code itself.
3. **If editing locally, run spellcheck and the build to identify any blocking errors** - Docs require build, spellcheck, and precommit CI/CD tests to pass before they can merge. This means that if you have a pull request with docs changes, a docs build error can prevent your code merge. Checking the build and spelling locally first can help speed up reviews. If you made formatting changes, checking a local build of your docs allows you to make sure you correctly formatted elements like tables, text styling, and line breaks.
4. **Make your pull request** - When you make a PR, Github automatically assigns reviewers and runs CI/CD tests.
5. **Fix any build errors or spelling mistakes** - Your PR can't be merged if there are any spelling or build errors. Check to see which builds are failing and click **Show details**. The output of the tests share the errors, location of the problems, and suggest resolutions. Common Docs failures occur due to incorrect formatting and whitespace.

Editing in GitHub or locally
----------------------------

You have two options for editing Airflow docs:

1. Through the online GitHub Editor by clicking **Suggest a change on this page** in the `docs <https://airflow.apache.org/docs/>`_, or by selecting a file in `GitHub <https://github.com/apache/airflow/tree/main/docs>`__.

2. Locally with a forked copy of the Airflow repo, where you can run local builds and tests prior to making a pull request.

+--------------------------------------+------------------+-------------------------------------------------+
|  Type of Docs update                 | Suggested Editor | Explanation                                     |
+======================================+==================+=================================================+
| I need to edit multiple files.       | Local Editor     | It's easier to batch-edit files in an editor,   |
|                                      |                  | than make multiple PRs or changes to individual |
|                                      |                  | files in a GitHub editor.                       |
+--------------------------------------+------------------+-------------------------------------------------+
| I want to fix a quick typo or a      | GitHub Editor    | Allows you to quickly edit without any local    |
| broken link.                         |                  | installation or build required.                 |
+--------------------------------------+------------------+-------------------------------------------------+
| My edits contain tables or           | Local Editor     | GitHub can provide Markdown previews, but might |
| other formatting changes.            |                  | change ``.rst`` styling. Use a local build.     |
+--------------------------------------+------------------+-------------------------------------------------+
| I want to make a new page/           | Local Editor     | Will need a local build to check navigation and |
| delete a page.                       |                  | link redirects.                                 |
+--------------------------------------+------------------+-------------------------------------------------+
| I want to edit autogenerated content | Either, probably | Allows you to easily find the correct file and  |
| on a page.                           | Local Editor     | generate a preview before creating the PR.      |
+--------------------------------------+------------------+-------------------------------------------------+

Finding source content to edit
------------------------------

Sphinx has _roles_ and _directives_, where Markdown docs frameworks often do not have similar functionality. This means that Airflow uses directives
to pull code examples, autogenerate indexes and tables of contents, and reference from resources across the codebase and across documentation provider packages.
This can make it confusing to find the content source on certain types of reference pages.

For example, in `Command Line Interface and Environment Variables Reference <https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html#environment-variables>`__, the CLI reference is `autogenerated <https://github.com/apache/airflow/blob/main/docs/apache-airflow/cli-and-env-variables-ref.rst?plain=1#L44>`__,
and requires more complex scripting. While the `Environment Variables <https://github.com/apache/airflow/blob/main/docs/apache-airflow/cli-and-env-variables-ref.rst?plain=1#L51>`__ are explicitly written.

To make an edit to an autogenerated doc, you need to make changes to a string in the Python source file. In the previous example, to make edits to the CLI command reference text, you must edit the `cli_config.py <https://github.com/apache/airflow/blob/main/airflow/cli/cli_config.py#L1861>`__ source file.

Building documentation
======================

To generate a local version of the docs you can use `<../dev/breeze/doc/README.rst>`_.

The documentation build consists of verifying consistency of documentation and two steps:

* spell checking
* building documentation

You can choose to run the complete build, to build all the docs and run spellcheck. Or, you can use package names and the optional flags, ``--spellcheck-only`` or ``--docs-only`` to choose the scope of the build.

Build all docs and spell check them:

.. code-block:: bash

    breeze build-docs

Just run spellcheck:

.. code-block:: bash

     breeze build-docs --spellcheck-only

Build docs without checking spelling:

.. code-block:: bash

     breeze build-docs --docs-only

Build documentation of just one provider package by calling the ``PACKAGE_ID``.

.. code-block:: bash

    breeze build-docs PACKAGE_ID

So, for example, to build just the ``apache-airflow-providers-apache-beam`` package docs, you would use the following:

.. code-block:: bash

    breeze build-docs apache.beam

Or, build docs for more than one provider package in the same command by listing multiple package IDs:

.. code-block:: bash

    breeze build-docs PACKAGE1_ID PACKAGE2_ID

You can also use the ``--package-filter`` flag to build docs for multiple packages that share a
common string. For example, to build docs for all the packages that start with
``apache-airflow-providers-apache-``, you would use the following:

.. code-block:: bash

    breeze build-docs --package-filter "apache-airflow-providers-apache-*"


You can see all the available arguments via ``--help``.

.. code-block:: bash

    breeze build-docs --help

While you can use full name of doc package starting with ``apache-airflow-providers-`` in package filter,
You can use shorthand version - just take the remaining part and replace every ``dash("-")`` with
a ``dot(".")``.

Example:
If the provider name is ``apache-airflow-providers-cncf-kubernetes``, it will be ``cncf.kubernetes``.

Note: For building docs for apache-airflow-providers index, use ``apache-airflow-providers`` as the
short hand operator.

Running the Docs Locally
------------------------

After you build the documentation, you can check the formatting, style, and documentation build at ``http://localhost:28080/docs/``
by starting a Breeze environment. Alternatively, you can run the following command from the root directory:

.. code-block:: bash

    docs/start_doc_server.sh

This command requires Python to be installed. This method is lighter on the system resources as you do not need to
launch the webserver just to view docs.

Once the server is running, you can view your documentation at http://localhost:8000. If you're using a virtual machine
like WSL2, you'll need to find the IP address of the WSL2 machine and replace "0.0.0.0" in your browser with it.
The address will look like http://n.n.n.n:8000, where n.n.n.n is the IP of the WSL2 machine.

Cross-referencing syntax
========================

Cross-references are generated by many semantically interpreted text roles.
Basically, you only need to write:

.. code-block:: rst

    :role:`target`

And Sphinx creates a link to the item named *target* of the type indicated by *role*. The link's
text is the same as *target*.

You can supply an explicit title and reference target, like in reST direct
hyperlinks:

.. code-block:: rst

    :role:`title <target>`

This will refer to *target*, but the link text will be *title*.

Here are practical examples:

.. code-block:: rst

    :class:`airflow.models.dag.DAG` - link to Python API reference documentation
    :doc:`/docs/operators` - link to other document
    :ref:`handle` - link to section in current or another document

    .. _handle:

    Section title
    ----------------------------------

Creating links between provider package docs
--------------------------------------------

Role ``:class:`` works well with references between packages. If you want to use other roles, it is a good idea to specify a package:

.. code-block:: rst

    :doc:`apache-airflow:installation/index`
    :ref:`apache-airflow-providers-google:write-logs-stackdriver`

If you still feel confused then you can view more possible roles for our documentation:

.. code-block:: bash

    ./list-roles.sh

For more information, see: `Cross-referencing syntax <https://www.sphinx-doc.org/en/master/usage/restructuredtext/roles.html>`_ in Sphinx documentation

Docs troubleshooting
====================

``example_dags`` Apache license
-------------------------------

If you are creating ``example_dags`` directory, you need to create an ``example_dags/__init__.py`` file. You can leave the file empty and the pre-commit processing
adds the license automatically. Otherwise, you can add a file with the Apache license or copy another ``__init__.py`` file that contains the necessary license.

Common Docs build errors
------------------------

.rst syntax is sensitive to whitespace, linebreaks, and indents, and can affect build output. When you write content and either
skip indentations, forget linebreaks, or leave trailing whitespace, it often produces docs build errors  that block your PR's mergeability.

unexpected unindent
*******************

Certain Sphinx elements, like lists and code blocks, require a blank line between the element and the next part of the content.
If you do not add a blank line, it creates a build error.

.. code-block:: text

    WARNING: Enumerated list ends without a blank line; unexpected unindent.

While easy to resolve, there's `a Sphinx bug <https://github.com/sphinx-doc/sphinx/issues/11026>`__ in certain versions that causes the
warning to report the wrong line in the file for your missing white space. If your PR has the ``unexpected unindent`` warning blocking your build,
and the line number it reports is wrong, this is a known error. You can find the missing blank space by searching for the syntax you used to make your
list, code block, or other whitespace-sensitive markup element.


spelling error with class or method name
****************************************
When a spelling error occurs that has a class/function/method name as incorrectly spelled,
instead of whitelisting it in docs/spelling_wordlist.txt you should make sure that
this name is quoted with backticks "`" - this should exclude it from spellchecking process.

In this example error, You should change the line with error so that the whole path is inside backticks "`".
.. code-block:: text

    Incorrect Spelling: 'BaseAsyncSessionFactory'
    Line with Error: ' airflow.providers.amazon.aws.hook.base_aws.BaseAsyncSessionFactory has been deprecated'

Support
========

If you need help, write to `#documentation <https://apache-airflow.slack.com/archives/CJ1LVREHX>`__ channel on `Airflow's Slack <https://s.apache.org/airflow-slack>`__.

For more resources about working with Sphinx or reST markup syntax, see the `Sphinx documentation <https://www.sphinx-doc.org/en/master/usage/quickstart.html>`__.

The `Write the Docs <https://www.writethedocs.org/slack/>`__ community also includes a #Sphinx Slack channel for questions and additional support.
