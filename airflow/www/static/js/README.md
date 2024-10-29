<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Grid View

In 2.3.0 the Tree view was completely rebuilt using React and renamed to Grid. Here is a primer on the new technologies used:

## [React](https://reactjs.org/)

The most popular javascript framework for building user interfaces with reusable components.
Written as javascript and html together in `.jsx` files.
In-component state can be managed via `useState()`, application state that spans many components can be managed via a context provider (see `/context` for examples), API state can be managed by React Query (see below)

## [Chakra UI](https://chakra-ui.com/)

A good component and helper function library. Tooltips, modals, toasts, switches, etc are all out of the box
Styles are applied via global theme when initializing the app or inline with individual components like `<Box padding="5px" />`

## [React Query](https://react-query.tanstack.com/)

A powerful async data handler that makes it easy to manage loading/error states as well as caching, refetching, background updates, etc.
This is our state management for any data that comes from an API.
Each API request is its own hook. Ie `useTasks` will get all the tasks for a Dag

## [React Testing Library](https://testing-library.com/docs/react-testing-library/intro/)

Easily write tests for react components and hooks
