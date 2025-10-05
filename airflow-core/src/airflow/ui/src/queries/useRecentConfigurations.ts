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
import { useQuery } from "@tanstack/react-query";
import axios from "axios";

export interface RecentConfiguration {
  run_id: string;
  conf: Record<string, unknown> | null;
  logical_date: string | null;
  start_date: string | null;
}

export interface RecentConfigurationsResponse {
  configurations: RecentConfiguration[];
}

const fetchRecentConfigurations = async (dagId: string, limit: number = 5): Promise<RecentConfigurationsResponse> => {
  const response = await axios.get<RecentConfigurationsResponse>(
    `/api/v2/dags/${dagId}/dagRuns/recent-configurations`,
    {
      params: { limit },
      withCredentials: true, // Use cookie authentication
    }
  );
  return response.data;
};

export const useRecentConfigurations = (dagId: string, limit: number = 5) => {
  return useQuery({
    queryKey: ["recentConfigurations", dagId, limit],
    queryFn: () => fetchRecentConfigurations(dagId, limit),
    enabled: !!dagId,
    staleTime: 5 * 60 * 1000, // 5 minutes
    retry: 2,
  });
};
