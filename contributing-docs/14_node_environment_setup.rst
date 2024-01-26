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

Node.js Environment Setup
=========================

``airflow/www/`` contains all yarn-managed, front-end assets. Flask-Appbuilder
itself comes bundled with jQuery and bootstrap. While they may be phased out
over time, these packages are currently not managed with yarn.

Make sure you are using recent versions of node and yarn. No problems have been
found with node\>=8.11.3 and yarn\>=1.19.1. The pre-commit framework of ours install
node and yarn automatically when installed - if you use ``breeze`` you do not need to install
neither node nor yarn.

.. contents:: :local:

Installing yarn and its packages manually
-----------------------------------------

To install yarn on macOS:

1.  Run the following commands (taken from `this source <https://gist.github.com/DanHerbert/9520689>`__):

.. code-block:: bash

    brew install node
    brew install yarn
    yarn config set prefix ~/.yarn


2.  Add ``~/.yarn/bin`` to your ``PATH`` so that commands you are installing
    could be used globally.

3.  Set up your ``.bashrc`` file and then ``source ~/.bashrc`` to reflect the
    change.

.. code-block:: bash

    export PATH="$HOME/.yarn/bin:$PATH"

4.  Install third-party libraries defined in ``package.json`` by running the following command

.. code-block:: bash

    yarn install

Generate Bundled Files with yarn
--------------------------------

To parse and generate bundled files for Airflow, run either of the following
commands:

.. code-block:: bash

    # Compiles the production / optimized js & css
    yarn run prod

    # Starts a web server that manages and updates your assets as you modify them
    # You'll need to run the webserver in debug mode too: ``airflow webserver -d``
    yarn run dev

Follow Style Guide
------------------

We try to enforce a more consistent style and follow the Javascript/Typescript community
guidelines.

Once you add or modify any JS/TS code in the project, please make sure it
follows the guidelines defined in `Airbnb
JavaScript Style Guide <https://github.com/airbnb/javascript>`__.

Apache Airflow uses `ESLint <https://eslint.org/>`__ as a tool for identifying and
reporting issues in JS/TS, and `Prettier <https://prettier.io/>`__ for code formatting.
Most IDE directly integrate with these tools, you can also manually run them with any of the following commands:

.. code-block:: bash

    # Format code in .js, .jsx, .ts, .tsx, .json, .css, .html files
    yarn format

    # Check JS/TS code in .js, .jsx, .ts, .tsx, .html files and report any errors/warnings
    yarn run lint

    # Check JS/TS code in .js, .jsx, .ts, .tsx, .html files and report any errors/warnings and fix them if possible
    yarn run lint:fix

    # Run tests for all .test.js, .test.jsx, .test.ts, test.tsx files
    yarn test

React, JSX and Chakra
-----------------------------

In order to create a more modern UI, we have started to include `React <https://reactjs.org/>`__ in the ``airflow/www/`` project.
If you are unfamiliar with React then it is recommended to check out their documentation to understand components and jsx syntax.

We are using `Chakra UI <https://chakra-ui.com/>`__ as a component and styling library. Notably, all styling is done in a theme file or
inline when defining a component. There are a few shorthand style props like ``px`` instead of ``padding-right, padding-left``.
To make this work, all Chakra styling and css styling are completely separate. It is best to think of the React components as a separate app
that lives inside of the main app.

------

If you happen to change architecture of Airflow, you can learn how we create our `Architecture diagrams <15_architecture_diagrams.rst>`__.
