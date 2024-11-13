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
import { useImportErrorServiceGetImportErrors } from "openapi/queries";

const queryOptions = {
  refetchOnMount: true,
  refetchOnReconnect: false,
  refetchOnWindowFocus: false,
  staleTime: 5 * 60 * 1000,
};

export const useImportErrors = (
  searchParams: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
) => {
  const { limit = 10, offset, orderBy } = searchParams;

  const {
    data: errorsData,
    error: errorsError,
    isFetching: isErrorsFetching,
    isLoading: isErrorsLoading,
  } = useImportErrorServiceGetImportErrors(
    { limit, offset, orderBy },
    undefined,
    queryOptions,
  );

  return {
    data: {
      errors: errorsData?.import_errors ?? [],
      total_entries: errorsData?.total_entries ?? 0,
    },
    error: errorsError,
    isFetching: isErrorsFetching,
    isLoading: isErrorsLoading,
  };
};
