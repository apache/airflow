// We need this lint rule for now because these are only dev-dependencies
/* eslint-disable import/no-extraneous-dependencies */
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

import '@testing-library/jest-dom';

// Mock global objects we use across the app
global.stateColors = {
  deferred: 'mediumpurple',
  failed: 'red',
  queued: 'gray',
  running: 'lime',
  scheduled: 'tan',
  skipped: 'hotpink',
  success: 'green',
  up_for_reschedule: 'turquoise',
  up_for_retry: 'gold',
  upstream_failed: 'orange',
};

global.defaultDagRunDisplayNumber = 245;
