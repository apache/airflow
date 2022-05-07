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

import axios from 'axios';
import camelcaseKeys from 'camelcase-keys';

import useTasks from './useTasks';
import useClearRun from './useClearRun';
import useQueueRun from './useQueueRun';
import useMarkFailedRun from './useMarkFailedRun';
import useMarkSuccessRun from './useMarkSuccessRun';
import useRunTask from './useRunTask';
import useClearTask from './useClearTask';
import useMarkFailedTask from './useMarkFailedTask';
import useMarkSuccessTask from './useMarkSuccessTask';
import useExtraLinks from './useExtraLinks';
import useConfirmMarkTask from './useConfirmMarkTask';
import useGridData from './useGridData';
import useMappedInstances from './useMappedInstances';

axios.interceptors.response.use(
  (res) => (res.data ? camelcaseKeys(res.data, { deep: true }) : res),
);

axios.defaults.headers.common.Accept = 'application/json';

export {
  useTasks,
  useClearRun,
  useQueueRun,
  useMarkFailedRun,
  useMarkSuccessRun,
  useRunTask,
  useClearTask,
  useMarkFailedTask,
  useMarkSuccessTask,
  useExtraLinks,
  useConfirmMarkTask,
  useGridData,
  useMappedInstances,
};
