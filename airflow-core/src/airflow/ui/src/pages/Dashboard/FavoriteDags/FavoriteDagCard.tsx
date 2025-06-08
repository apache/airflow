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
import { Box, Button, Text, VStack } from "@chakra-ui/react";
import { Link as RouterLink } from "react-router-dom";

import { useDagServiceRecentDagRuns } from "openapi/queries";
import { RecentRuns } from "src/pages/DagsList/RecentRuns";

type FavoriteDagProps = {
  readonly dagId: string;
};

export const FavoriteDagCard = ({ dagId }: FavoriteDagProps) => {
  const { data } = useDagServiceRecentDagRuns({
    dagIds: [dagId],
    dagRunsLimit: 5,
  });

  const latestRuns = data?.dags[0]?.latest_dag_runs ?? [];

  return (
    <Box width="100%">
      <RouterLink to={`/dags/${dagId}`}>
        <Button
          borderRadius="md"
          display="flex"
          flexDirection="column"
          gap={2}
          height="auto"
          px={4}
          py={3}
          variant="outline"
          width="100%"
        >
          <Box mt={1}>
            <VStack>
              <RecentRuns latestRuns={latestRuns} />
              <Text _hover={{ textDecoration: "underline" }} fontSize="sm" textAlign="left">
                {dagId}
              </Text>
            </VStack>
          </Box>
        </Button>
      </RouterLink>
    </Box>
  );
};
