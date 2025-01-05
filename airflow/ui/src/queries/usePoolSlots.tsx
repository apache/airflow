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

import { usePoolServiceGetPools } from 'openapi/queries';
import type { PoolResponse } from 'openapi/requests/types.gen';

export type PoolWithStats = {
  running: number;
  queued: number;
  deferred: number;
  scheduled: number;
  open: number;
} & PoolResponse;

export const usePools = (
  searchParams: {
    poolNamePattern?: string;
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
) => {
  const { 
    data, 
    error, 
    isFetching,
    isLoading 
  } = usePoolServiceGetPools(searchParams);

  const pools = (data?.pools ?? []).map((pool) => {
    const totalSlots = pool.slots || 0;
    const running = pool.occupied_slots || 0;
    const queued = pool.queued_slots || 0;
    const deferred = pool.deferred_slots || 0;
    const scheduled = pool.scheduled_slots || 0;
    const open = totalSlots - (running + queued + deferred + scheduled);

    return {
      ...pool,
      running,
      queued,
      deferred,
      scheduled,
      open,
    };
  });

  return {
    data: { pools, total_entries: data?.total_entries ?? 0 },
    error,
    isFetching,
    isLoading,
  };
};