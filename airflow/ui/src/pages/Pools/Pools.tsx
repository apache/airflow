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
import { Box, Skeleton } from "@chakra-ui/react";

import { usePoolServiceGetPools } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";

import PoolBar from "./PoolBar";

export const Pools = () => {
  const { data, error, isLoading } = usePoolServiceGetPools();

  return isLoading ? (
    <Skeleton height="30" />
  ) : (
    <>
      <ErrorAlert error={error} />
      <Box p={2}>
        <PoolBar pools={data?.pools ?? []} />
      </Box>
    </>
  );
};
