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
import { useDagsServiceRecentDagRuns } from "openapi/queries";
import { Link as RouterLink } from "react-router-dom";
import { RecentRuns } from "src/pages/DagsList/RecentRuns";

type FavoriteDagProps = {
  readonly dag_id: string;
};

export const FavoriteDagCard = ({ dag_id }: FavoriteDagProps) => {

  const { data } = useDagsServiceRecentDagRuns({
    dagIds: [dag_id],
    dagRunsLimit: 5,
  });

  const latestRuns = data?.dags[0]?.latest_dag_runs || [];
  
  return (
    <RouterLink to={`/dags/${dag_id}`} >
      <Button borderRadius="md" display="flex" gap={2} variant="outline" flexDirection="column" px={4} py={3} height="auto">
        <Box mt={1}>
          <VStack>
            <RecentRuns latestRuns={latestRuns} />
            <Text fontSize="sm" textAlign="left" _hover={{ textDecoration: "underline" }}>
              {dag_id}
            </Text>
          </VStack>
        </Box>
      </Button>
    </RouterLink>  
  );
};
