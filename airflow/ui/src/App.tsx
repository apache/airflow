/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { hot } from 'react-hot-loader';
import React from 'react';
import { Route, Redirect, Switch } from 'react-router-dom';

import PrivateRoute from 'auth/PrivateRoute';

import Pipelines from 'views/Pipelines';
import Pipeline from 'views/Pipeline';

import EventLogs from 'views/Activity/EventLogs';

import Config from 'views/Config';

import Access from 'views/Access';
import Users from 'views/Access/Users';
import Roles from 'views/Access/Roles';

import Docs from 'views/Docs';
import NotFound from 'views/NotFound';

const App = () => (
  <Switch>
    <Redirect exact path="/" to="/pipelines" />
    <PrivateRoute exact path="/pipelines" component={Pipelines} />
    <PrivateRoute exact path="/pipelines/:dagId" component={Pipeline} />

    <PrivateRoute exact path="/activity/event-logs" component={EventLogs} />

    <PrivateRoute exact path="/config" component={Config} />

    <PrivateRoute exact path="/access" component={Access} />
    <PrivateRoute exact path="/access/users" component={Users} />
    <PrivateRoute exact path="/access/users/new" component={Users} />
    <PrivateRoute exact path="/access/users/:username" component={Users} />
    <PrivateRoute exact path="/access/users/:username/edit" component={Users} />
    <PrivateRoute exact path="/access/roles" component={Roles} />

    <Route exact path="/docs" component={Docs} />

    <Route component={NotFound} />
  </Switch>
);

export default hot(module)(App);
