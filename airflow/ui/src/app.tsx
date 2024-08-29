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

import { Box, Spinner } from "@chakra-ui/react";

import { useDagServiceGetDags } from "openapi/queries";
import { DagsList } from "src/dagsList";
import { Nav } from "src/nav";

export const App = () => {
  const { data, isLoading } = useDagServiceGetDags();

  return (
    <Box maxWidth="100vw">
      <Nav />
      <Box p={3}>
        {isLoading && <Spinner />}
        {!isLoading && !!data?.dags && <DagsList data={data.dags} />}
      </Box>
    </Box>
  );
};

export const AppSimple = () => <div>Something</div>;
