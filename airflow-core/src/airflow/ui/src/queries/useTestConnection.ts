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
import { useQueryClient } from "@tanstack/react-query";
import type { Dispatch, SetStateAction } from "react";

import { useConnectionServiceTestConnection, useConnectionServiceGetConnectionsKey } from "openapi/queries";
import type { ConnectionTestResponse } from "openapi/requests/types.gen";

export const useTestConnection = (
  setConnected: Dispatch<SetStateAction<boolean | undefined>>,
  setMessage: Dispatch<SetStateAction<string | undefined>>
) => {
  const queryClient = useQueryClient();

  const onSuccess = async (res: ConnectionTestResponse) => {
    await queryClient.invalidateQueries({
      queryKey: [useConnectionServiceGetConnectionsKey],
    });
    setConnected(res.status);
    setMessage(res.message);
  };

  const onError = (error: any) => {
    setConnected(false);
    
    // Extract error message from different possible error structures
    let errorMessage = "Connection test failed";
    
    // Try different error message extraction strategies
    if (error?.body?.detail) {
      errorMessage = error.body.detail;
    } else if (error?.body?.message) {
      errorMessage = error.body.message;
    } else if (error?.response?.data?.detail) {
      errorMessage = error.response.data.detail;
    } else if (error?.response?.data?.message) {
      errorMessage = error.response.data.message;
    } else if (error?.message) {
      errorMessage = error.message;
    } else if (typeof error?.body === 'string') {
      errorMessage = error.body;
    } else if (error?.body && typeof error.body === 'object') {
      // Try to extract message from nested error object
      const bodyStr = JSON.stringify(error.body);
      if (bodyStr.includes('detail')) {
        try {
          const parsed = JSON.parse(bodyStr);
          errorMessage = parsed.detail || parsed.message || errorMessage;
        } catch (e) {
          // If parsing fails, use the string representation
          errorMessage = bodyStr;
        }
      }
    }
    
    setMessage(errorMessage);
  };

  return useConnectionServiceTestConnection({
    onError,
    onSuccess,
  });
};
